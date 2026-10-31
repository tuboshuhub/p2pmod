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

    private final boolean fecEnabled;
    private final int fecK;
    private final int fecR;
    private final Object fecLock = new Object();
    private final java.util.Map<Long, FecBlock> fecBlocks = new java.util.LinkedHashMap<>();

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

    public Receiver(ControlSink ctrl, Feedback feedback, AppSink app, boolean fecEnabled, int fecK, int fecR) {
        this.ctrl = ctrl;
        this.feedback = feedback;
        this.app = app;
        int k = Math.max(1, Math.min(255, fecK));
        int r = Math.max(0, Math.min(255 - k, fecR));
        this.fecEnabled = fecEnabled;
        this.fecK = k;
        this.fecR = r;
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
            if (fecEnabled) noteFecSource(seq, payload);
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

    private void noteFecSource(long seq, byte[] payload) {
        int K = fecK;
        long blockId = seq / K;
        int pos = (int) (seq % K);
        FecBlock b = block(blockId);
        if (b.k != K) return;
        if (pos < b.k) {
            System.arraycopy(payload, 0, b.matrix[pos], 0, payload.length);
            b.lengths[pos] = payload.length;
            if (!b.present[pos]) { b.present[pos] = true; b.got++; }
            if (pos + 1 > b.kactual) b.kactual = pos + 1;
        }
        tryFec(blockId);
    }

    public void ingestFec(byte[] body) {
        if (!fecEnabled || body == null || body.length < 12 + 2 * fecK) return;
        long blockId = 0;
        for (int i = 0; i < 8; i++) blockId = (blockId << 8) | (body[i] & 0xFF);
        int parityIndex = body[8] & 0xFF;
        int K = body[9] & 0xFF;
        int R = body[10] & 0xFF;
        int kactual = body[11] & 0xFF;
        if (K != fecK || R != fecR) return;
        int[] lens = new int[K];
        int o = 12;
        for (int j = 0; j < K; j++) {
            lens[j] = ((body[o] & 0xFF) << 8) | (body[o + 1] & 0xFF);
            o += 2;
        }
        int pLen = body.length - o;
        if (pLen != Mux.FRAG_SIZE) return;
        byte[] parity = new byte[pLen];
        System.arraycopy(body, o, parity, 0, pLen);
        FecBlock b = block(blockId);
        if (b.k != K) return;
        for (int j = 0; j < K; j++) b.lengths[j] = lens[j];
        if (kactual < b.kactual) b.kactual = kactual;
        if (parityIndex < R) {
            System.arraycopy(parity, 0, b.matrix[K + parityIndex], 0, pLen);
            if (!b.present[K + parityIndex]) { b.present[K + parityIndex] = true; b.got++; }
        }
        tryFec(blockId);
    }

    private FecBlock block(long blockId) {
        synchronized (fecLock) {
            FecBlock b = fecBlocks.get(blockId);
            if (b == null) {
                b = new FecBlock(fecK, fecR);
                fecBlocks.put(blockId, b);
                if (fecBlocks.size() > 256) {
                    java.util.Iterator<Long> it = fecBlocks.keySet().iterator();
                    if (it.hasNext()) fecBlocks.remove(it.next());
                }
            }
            return b;
        }
    }

    private void tryFec(long blockId) {
        java.util.List<Long> seqs = null;
        java.util.List<byte[]> payloads = null;
        synchronized (fecLock) {
            FecBlock b = fecBlocks.get(blockId);
            if (b == null || b.done) return;
            int missing = 0;
            for (int pos = 0; pos < b.kactual; pos++) if (!b.present[pos]) missing++;
            if (missing == 0) { b.done = true; fecBlocks.remove(blockId); return; }
            if (b.got < b.k) return;
            byte[][] rec = Fec.decode(b.matrix, b.present, b.k, b.r);
            if (rec == null) return;
            seqs = new java.util.ArrayList<>();
            payloads = new java.util.ArrayList<>();
            for (int pos = 0; pos < b.kactual; pos++) {
                if (!b.present[pos]) {
                    int len = b.lengths[pos];
                    byte[] out = new byte[len];
                    System.arraycopy(rec[pos], 0, out, 0, len);
                    seqs.add(blockId * b.k + pos);
                    payloads.add(out);
                }
            }
        }
        if (seqs != null) {
            for (int i = 0; i < seqs.size(); i++) route(seqs.get(i), payloads.get(i));
            synchronized (fecLock) {
                FecBlock b = fecBlocks.get(blockId);
                if (b != null) { b.done = true; fecBlocks.remove(blockId); }
            }
        }
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

    private static final class FecBlock {
        final int k, r;
        final byte[][] matrix;
        final boolean[] present;
        final int[] lengths;
        int kactual;
        int got;
        boolean done;
        FecBlock(int k, int r) {
            this.k = k;
            this.r = r;
            matrix = new byte[k + r][Mux.FRAG_SIZE];
            present = new boolean[k + r];
            lengths = new int[k];
            kactual = k;
            got = 0;
        }
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
