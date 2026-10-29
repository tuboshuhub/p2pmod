package dev.p2pmod.core;

import dev.p2pmod.support.Trace;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class Receiver {

    public interface ControlSink {
        void onAck(long cumulative);
        void onNack(long missingSeq);
    }

    public interface Feedback {
        void ack(long cumulative);
        void nack(long missingSeq);
    }

    public interface AppSink { void accept(byte[] payload); }

    private static final int MAX_PENDING = 4096;

    private static final int DELIVER_BYTES_CAP = 32 * 1024 * 1024;

    private static final long NACK_INTERVAL_MS = 80;

    private static final long GAP_GRACE_MS = 80;

    private static final int ACK_EVERY = 4;

    private final ControlSink ctrl;
    private final Feedback feedback;
    private final AppSink app;

    private final Object bufLock = new Object();
    private byte[] acc = new byte[8192];
    private int accLen;

    private final Object seqLock = new Object();
    private long expectSeq;
    private long delivered;
    private final TreeMap<Long, byte[]> pending = new TreeMap<>();
    private volatile long lastProgressMs = System.currentTimeMillis();

    private long lastNackMs;

    private long gapSinceMs;
    private final LinkedBlockingDeque<byte[]> delivery = new LinkedBlockingDeque<>();
    private final AtomicInteger deliveryBytes = new AtomicInteger();
    private final Thread courier;
    private volatile boolean closed;

    private final AtomicLong late = new AtomicLong();
    private final AtomicLong future = new AtomicLong();
    private final AtomicLong corrupt = new AtomicLong();

    public Receiver(ControlSink ctrl, Feedback feedback, AppSink app) {
        this.ctrl = ctrl;
        this.feedback = feedback;
        this.app = app;
        this.courier = new Thread(this::deliverLoop, "p2p-rx-deliver");
        this.courier.setDaemon(true);
        this.courier.start();
    }

    public void ingest(byte[] chunk) {
        if (closed || chunk == null || chunk.length == 0) return;
        synchronized (bufLock) {
            if (accLen + chunk.length > acc.length) grow(accLen + chunk.length);
            System.arraycopy(chunk, 0, acc, accLen, chunk.length);
            accLen += chunk.length;
            parse();
        }
    }

    private void grow(int need) {
        int n = acc.length;
        while (n < need) n <<= 1;
        byte[] bigger = new byte[n];
        System.arraycopy(acc, 0, bigger, 0, accLen);
        acc = bigger;
    }

    private void parse() {
        int off = 0;
        while (accLen - off >= Mux.HEADER) {
            long seq = Mux.seqOf(acc, off);
            int len = Mux.lenOf(acc, off);
            if (len < 0 || len > Mux.MAX_PAYLOAD) {

                int dropped = accLen;
                accLen = 0;
                corrupt.incrementAndGet();

                Trace.periodic("[P2P] Byte stream misaligned (read length {}, over the cap), dropped {} bytes to realign",
                        len, dropped);
                return;
            }
            if (accLen - off - Mux.HEADER < len) break;
            byte[] payload = new byte[len];
            System.arraycopy(acc, off + Mux.HEADER, payload, 0, len);
            off += Mux.HEADER + len;
            route(seq, payload);
        }
        int remaining = accLen - off;
        if (remaining > 0) System.arraycopy(acc, off, acc, 0, remaining);
        accLen = remaining;
        if (accLen == 0 && acc.length > 64 * 1024) acc = new byte[8192];
    }

    private void route(long seq, byte[] payload) {
        if (seq == Mux.CTRL_SEQ) {
            if (payload.length < 9) return;
            byte kind = Mux.ctrlKindOf(payload);
            long v = Mux.ctrlValueOf(payload);
            if (kind == Mux.CTRL_ACK) ctrl.onAck(v);
            else ctrl.onNack(v);
            return;
        }
        dispatch(seq, payload);
    }

    private void dispatch(long seq, byte[] payload) {
        List<byte[]> ready = null;
        boolean gapFilled = false;
        long jumpedTo = -1;
        synchronized (seqLock) {
            if (seq == expectSeq) {

                gapFilled = !pending.isEmpty();
                ready = new ArrayList<>(2);
                ready.add(payload);
                expectSeq++;
                drain(ready);
                if (gapFilled) jumpedTo = expectSeq - 1;

                gapSinceMs = pending.isEmpty() ? 0 : System.currentTimeMillis();
            } else if (seq > expectSeq) {
                if (pending.size() >= MAX_PENDING) {
                    future.incrementAndGet();
                } else {
                    pending.putIfAbsent(seq, payload);
                }
                if (gapSinceMs == 0) gapSinceMs = System.currentTimeMillis();
                ask();
            } else {
                late.incrementAndGet();
            }
        }
        if (ready != null) {
            lastProgressMs = System.currentTimeMillis();
            for (byte[] p : ready) handUp(p);
        }

        if (jumpedTo >= 0) feedback.ack(jumpedTo);
        else if (++delivered % ACK_EVERY == 0) {
            synchronized (seqLock) { feedback.ack(expectSeq - 1); }
        }
    }

    private void drain(List<byte[]> sink) {
        while (true) {
            byte[] next = pending.remove(expectSeq);
            if (next == null) break;
            expectSeq++;
            sink.add(next);
        }
    }

    private void ask() {
        long now = System.currentTimeMillis();
        if (gapSinceMs == 0 || now - gapSinceMs < GAP_GRACE_MS) return;
        if (now - lastNackMs < NACK_INTERVAL_MS) return;
        lastNackMs = now;
        feedback.nack(expectSeq);
    }

    public void pokeNack() {
        synchronized (seqLock) {
            if (!pending.isEmpty()) ask();
        }
    }

    public long cumulative() {
        synchronized (seqLock) { return expectSeq - 1; }
    }

    public long progressAgeMs() {
        return System.currentTimeMillis() - lastProgressMs;
    }

    private void handUp(byte[] payload) {
        if (closed) return;
        deliveryBytes.addAndGet(payload.length);
        delivery.offer(payload);

        while (!closed && deliveryBytes.get() > DELIVER_BYTES_CAP) {
            sleepQuietly(5);
        }
    }

    private void deliverLoop() {
        while (!closed) {
            byte[] payload;
            try {
                payload = delivery.take();
            } catch (InterruptedException e) {
                return;
            }
            deliveryBytes.addAndGet(-payload.length);
            try {
                app.accept(payload);
            } catch (Throwable ignored) {
            }
        }
    }

    public boolean drained() {
        return delivery.isEmpty() && pending.isEmpty();
    }

    public String stats() {
        return String.format("expect=%d pending=%d deliver=%d late=%d future=%d corrupt=%d",
                expectSeq, pending.size(), delivery.size(), late.get(), future.get(), corrupt.get());
    }

    public void close() {
        closed = true;
        courier.interrupt();
        delivery.clear();
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
