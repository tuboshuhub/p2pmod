package dev.p2pmod.core;

import dev.p2pmod.support.Trace;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

public final class Sender {

    public interface Route { Path path(); }

    private static final int QUEUE_CAP = 8192;

    private static final long OFFER_TIMEOUT_MS = 2_000;

    private static final long COMMIT_WINDOW_WAIT_MS = 20_000;
    private static final int PARK_MAX_BYTES = 8 * 1024 * 1024;
    private static final int PARK_BATCH = 128;

    private static final int SEND_WINDOW = 8192;

    private static final int ABSOLUTE_MAX_FRAMES = 65_536;
    private static final int MAX_NACK_WANTED = 4096;
    private static final long NACK_TTL_MS = 30_000;

    private static final long RTO_FLOOR_MS = 250;
    private static final long RTO_CEIL_MS = 3_000;

    private static final int MAX_RETRIES = 10;
    private static final long SLOW_RETRY_MS = 5_000;
    private static final int BURST_CAP = 128;

    private static final int NACK_BURST = 8;

    private static final long NACK_RESEND_MIN_MS = 120;

    private static final int NACK_BURST_RESEND = 6;

    private final int connId;
    private final Route route;

    private final boolean fecEnabled;
    private final int fecK;
    private final int fecR;
    private final Object fecLock = new Object();
    private final java.util.ArrayDeque<byte[]> fecPending = new java.util.ArrayDeque<>();
    private long fecBlockBase = -1;
    private long fecBlockStartMs = 0;
    private static final long FEC_FLUSH_MS = 25;

    private final Object lock = new Object();
    private long nextSeq;
    private long lastAcked = -1;
    private final LinkedHashMap<Long, Record> inflight = new LinkedHashMap<>();

    private final LinkedHashMap<Long, Long> nackWanted = new LinkedHashMap<>();
    private long lastAckMs = System.currentTimeMillis();

    private long windowFullSinceMs;

    private volatile int cwnd = CWND_INIT;
    private long lastCwndCutMs;
    private long lastCwndGrowMs;

    private long nextSlotUs;

    private long nackSeen;
    private long framesSent;

    private static final long LOSS_DISPLAY_MS = 10_000;

    private static final long MIN_LOSS_SAMPLE = 20;
    private long dispLossMs;
    private long dispLossSent;
    private long dispLossLost;
    private volatile int dispTenths;

    private long firstResend;

    private long suppressLossUntilMs;
    private static final long SWITCH_TAIL_MS = 3_000;

    private long lossWinStartMs;
    private long lossWinSent;
    private long lossWinFresh;
    private static final long LOSS_WINDOW_MS = 1_000;

    private static final int LOSS_CUT_PCT = 5;

    private volatile int ackedRate;
    private long rateSampleStartMs;
    private long rateSampleFrames;

    private long rateSampleSentStart;

    private volatile double paceTokens;
    private volatile long paceLastUs;

    private static final double PACE_BURST = 24.0;

    private static final int WINDOW_SLACK = 512;

    private static final int CWND_INIT = 1024;

    private static final int CWND_MIN = 128;

    private static final int CWND_CUT_NUM = 2;
    private static final int CWND_CUT_DEN = 3;

    private static final long CWND_CUT_GAP_MS = 150;

    private static final long CWND_GROW_GAP_MS = 100;
    private static final int CWND_GROW_MIN = 16;

    private final LinkedBlockingDeque<byte[]> queue = new LinkedBlockingDeque<>(QUEUE_CAP);
    private final ArrayDeque<byte[]> parked = new ArrayDeque<>();
    private int parkedBytes;

    private final Thread flusher;
    private volatile boolean closed;

    private static final class Record {
        final byte[] frame;
        long sentAt = System.currentTimeMillis();
        int retries;
        Record(byte[] frame) { this.frame = frame; }
    }

    public Sender(int connId, Route route, boolean fecEnabled, int fecK, int fecR) {
        this.connId = connId;
        this.route = route;
        int k = Math.max(1, Math.min(255, fecK));
        int r = Math.max(0, Math.min(255 - k, fecR));
        this.fecEnabled = fecEnabled;
        this.fecK = k;
        this.fecR = r;
        this.flusher = new Thread(this::flushLoop, "p2p-tx-" + connId);
        this.flusher.setDaemon(true);
        this.flusher.start();
    }

    public void send(byte[] data) throws IOException {
        send(data, 0, data.length);
    }

    public void send(byte[] data, int off, int len) throws IOException {
        if (closed) throw new IOException("sender closed");
        if (off < 0 || len < 0 || off + len > data.length) {
            throw new IOException("bad slice off=" + off + " len=" + len);
        }
        int end = off + len;
        boolean committed = false;
        while (off < end) {
            int n = Math.min(Mux.FRAG_SIZE, end - off);

            reserve(committed);
            byte[] frame;
            long seq;
            synchronized (lock) {
                seq = nextSeq++;
                frame = Mux.frame(seq, data, off, n);

                if (inflight.isEmpty()) lastAckMs = System.currentTimeMillis();
                inflight.put(seq, new Record(frame));
                trim();
            }

            offer(frame);
            if (fecEnabled) {
                byte[] pay = new byte[n];
                System.arraycopy(data, off, pay, 0, n);
                bufferFec(seq, pay);
            }
            off += n;
            committed = true;
        }
    }

    private void reserve(boolean committed) throws IOException {
        long budget = committed ? COMMIT_WINDOW_WAIT_MS : OFFER_TIMEOUT_MS;
        long deadline = System.currentTimeMillis() + budget;
        while (true) {
            synchronized (lock) {

                if (inflight.size() < cwnd) {

                    if (inflight.size() < SEND_WINDOW - WINDOW_SLACK) windowFullSinceMs = 0;
                    return;
                }

                if (inflight.size() >= SEND_WINDOW && windowFullSinceMs == 0) {
                    windowFullSinceMs = System.currentTimeMillis();
                }
            }
            if (closed) throw new IOException("sender closed");
            if (System.currentTimeMillis() >= deadline) {
                if (committed) {

                    closed = true;
                    flusher.interrupt();
                    Trace.warn("[P2P] conn={} send window did not recover within {}ms ({} fragments unacked, cwnd={}), closing the connection to avoid byte-stream misalignment",
                            connId, budget, inflight.size(), cwnd);
                    throw new IOException("sender closed: send window full for too long");
                }
                throw new RetryableIOException("send window still full after " + OFFER_TIMEOUT_MS
                        + "ms (" + cwnd + " fragments unacked) -- the caller should pause reading and retry",
                        RetryableIOException.WINDOW_FULL_BUDGET_MS);
            }
            sleepQuietly(20);
        }
    }

    public void tick() {
        maybeFlushFec();
        Path p = route.path();
        if (p == null || !p.usable()) return;
        long now = System.currentTimeMillis();
        long base = rtoFor(p);
        int resent = 0;
        synchronized (lock) {

            int urgent = 0;
            for (Long s : nackWanted.keySet()) {
                if (urgent >= NACK_BURST || resent >= BURST_CAP) break;
                Record r = inflight.get(s);
                if (r == null || s <= lastAcked) continue;
                if (now - r.sentAt < NACK_RESEND_MIN_MS) continue;
                try {
                    p.send(packet(r.frame));
                    r.sentAt = now;
                    noteResend(r);
                    urgent++;
                    resent++;
                } catch (IOException ex) {
                    break;
                }
            }

            long gap = Long.MAX_VALUE;
            for (Long s : nackWanted.keySet()) {

                if (s < gap && inflight.containsKey(s)) gap = s;
            }
            for (Map.Entry<Long, Record> e : inflight.entrySet()) {
                long seq = e.getKey();
                if (seq <= lastAcked) continue;

                if (seq > gap) break;
                Record r = e.getValue();
                long wait = r.retries >= MAX_RETRIES ? SLOW_RETRY_MS : backoff(base, r.retries);
                if (now - r.sentAt <= wait) continue;
                try {
                    p.send(packet(r.frame));
                    r.sentAt = now;
                    noteResend(r);
                    resent++;
                } catch (IOException ex) {
                    break;
                }
                if (resent >= BURST_CAP) break;
            }
        }
        if (resent > 0) {
            Trace.periodic("[P2P] conn={} retransmitted {} fragments (RTO={}ms, via {})", connId, resent, base, p.label());
        }
    }

    public void onAck(long cumulative) {
        synchronized (lock) {
            if (cumulative <= lastAcked) return;
            long now = System.currentTimeMillis();
            long advanced = lastAcked < 0 ? cumulative + 1 : cumulative - lastAcked;
            lastAcked = cumulative;
            lastAckMs = now;

            var it = inflight.entrySet().iterator();
            while (it.hasNext()) {
                if (it.next().getKey() > cumulative) break;
                it.remove();
            }
            if (!nackWanted.isEmpty()) nackWanted.keySet().removeIf(s -> s <= cumulative);
            if (inflight.size() < SEND_WINDOW - WINDOW_SLACK) windowFullSinceMs = 0;
            sampleRate(now, advanced);
            growCwnd(now);
        }
    }

    private void sampleRate(long now, long advanced) {
        if (rateSampleStartMs == 0) {
            rateSampleStartMs = now;
            rateSampleFrames = 0;
            rateSampleSentStart = framesSent;
            return;
        }
        rateSampleFrames += advanced;
        long span = now - rateSampleStartMs;
        if (span < 500) return;
        long sent = framesSent - rateSampleSentStart;
        int r = (int) Math.min(Integer.MAX_VALUE, rateSampleFrames * 1000 / span);

        boolean appLimited = sent < Math.max(64, cwnd / 8);
        if (!appLimited) {
            ackedRate = Math.max(r, (int) (ackedRate * 0.9));
        }
        rateSampleStartMs = now;
        rateSampleFrames = 0;
        rateSampleSentStart = framesSent;
    }

    private void growCwnd(long now) {
        if (now - lastCwndGrowMs < CWND_GROW_GAP_MS) return;
        int c = cwnd;
        if (c >= SEND_WINDOW) return;
        lastCwndGrowMs = now;
        cwnd = Math.min(SEND_WINDOW, c + Math.max(CWND_GROW_MIN, c / 8));
    }

    private void cutCwnd(long now) {
        if (lastCwndCutMs == 0) lastCwndCutMs = now - CWND_CUT_GAP_MS;
        if (lossWinStartMs == 0) {
            lossWinStartMs = now;
            return;
        }
        long span = now - lossWinStartMs;
        if (span < LOSS_WINDOW_MS) return;
        double pct = lossWinSent > 0 ? lossWinFresh * 100.0 / lossWinSent : 100.0;
        lossWinStartMs = now;
        lossWinSent = 0;
        lossWinFresh = 0;
        if (pct < LOSS_CUT_PCT) return;
        if (now - lastCwndCutMs < CWND_CUT_GAP_MS) return;
        int c = cwnd;
        int next = Math.max(CWND_MIN, c * CWND_CUT_NUM / CWND_CUT_DEN);
        if (next >= c) return;
        lastCwndCutMs = now;
        cwnd = next;

        paceTokens = Math.min(paceTokens, PACE_BURST / 2);
        Trace.periodic("[P2P] conn={} loss {}%, window {} -> {} fragments", connId, (int) pct, c, next);
    }

    public int lossTenths() {
        synchronized (lock) {
            long now = System.currentTimeMillis();
            if (now - dispLossMs >= LOSS_DISPLAY_MS) {
                long sent = framesSent - dispLossSent;
                long lost = firstResend - dispLossLost;
                dispLossMs = now;
                dispLossSent = framesSent;
                dispLossLost = firstResend;
                if (sent >= MIN_LOSS_SAMPLE) {
                    long sample = Math.round(lost * 1000.0 / sent);
                    long next = Math.round(dispTenths * 0.4 + sample * 0.6);

                    if (lost > 0 && next == 0) next = 1;
                    dispTenths = (int) next;
                }
            }
            return dispTenths;
        }
    }

    private void noteResend(Record r) {

        if (r.retries == 0 && System.currentTimeMillis() >= suppressLossUntilMs) firstResend++;
        r.retries++;
    }

    public int lossPercent() {
        synchronized (lock) {
            if (framesSent <= 0) return 0;
            return (int) Math.min(100, nackSeen * 100 / framesSent);
        }
    }

    private static boolean isControl(byte[] f) {
        if (f == null || f.length < Mux.HEADER) return false;
        for (int i = 0; i < 8; i++) {
            if (f[i] != (byte) 0xFF) return false;
        }
        return true;
    }

    private void pace() {
        Path p = route.path();
        if (p == null || !p.usable()) return;
        int c = cwnd;
        int rtt = p.rtt() > 0 ? p.rtt() : 50;

        double rate = ackedRate > 0 ? ackedRate * 1.25 : c * 1000.0 / Math.max(10, rtt);

        if (rate < 1_500) rate = 1_500;

        if (rate > 12_000) rate = 12_000;

        long now = System.nanoTime() / 1000;
        if (paceLastUs == 0) {
            paceLastUs = now;
            paceTokens = PACE_BURST;
        }
        long dt = now - paceLastUs;
        paceLastUs = now;
        paceTokens = Math.min(PACE_BURST, paceTokens + dt * rate / 1_000_000.0);
        if (paceTokens >= 1.0) {
            paceTokens -= 1.0;
            return;
        }
        long waitMs = (long) Math.ceil((1.0 - paceTokens) * 1_000_000.0 / rate / 1000.0);
        if (waitMs > 0) sleepQuietly(Math.min(20, waitMs));
    }

    public long windowFullMs() {
        synchronized (lock) {
            return windowFullSinceMs == 0 ? 0 : System.currentTimeMillis() - windowFullSinceMs;
        }
    }

    public void onNack(long seq) {
        Path p = route.path();
        if (p == null || !p.usable()) return;
        synchronized (lock) {
            if (seq <= lastAcked) return;
            long now = System.currentTimeMillis();
            if (nackWanted.put(seq, now) == null) {

                nackSeen++;
                lossWinFresh++;
            }
            cutCwnd(now);

            evictBelow(seq);

            for (long s = seq; s < seq + NACK_BURST_RESEND; s++) {
                Record r = inflight.get(s);
                if (r == null || s <= lastAcked) continue;
                try {
                    p.send(packet(r.frame));
                    r.sentAt = now;
                    noteResend(r);
                } catch (IOException ignored) {
                    break;
                }
            }
        }
    }

    private void evictBelow(long seq) {
        Iterator<Map.Entry<Long, Record>> it = inflight.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Record> e = it.next();
            if (e.getKey() >= seq) return;
            it.remove();
        }
    }

    public void emitAck(long cumulative) {
        if (closed) return;
        queue.offer(Mux.control(Mux.CTRL_ACK, cumulative));
    }

    public void emitNack(long seq) {
        if (closed) return;
        queue.offer(Mux.control(Mux.CTRL_NACK, seq));
    }

    public int cwnd() { return cwnd; }

    public int ackedRate() { return ackedRate; }

    public void onPathChanged() {
        synchronized (lock) {
            suppressLossUntilMs = System.currentTimeMillis() + SWITCH_TAIL_MS;
            cwnd = CWND_INIT;
            lastCwndCutMs = 0;
            lastCwndGrowMs = 0;
            ackedRate = 0;
            rateSampleStartMs = 0;
            rateSampleFrames = 0;
            rateSampleSentStart = 0;
            lossWinStartMs = 0;
            lossWinSent = 0;
            lossWinFresh = 0;
            paceTokens = PACE_BURST;
            paceLastUs = 0;
        }
    }

    public int unacked() {
        synchronized (lock) {

            return inflight.size();
        }
    }

    public long ackSilenceMs() {
        return System.currentTimeMillis() - lastAckMs;
    }

    public void releaseParked() {
        Path p = route.path();
        if (p == null || !p.usable()) return;
        while (!closed) {
            List<byte[]> batch = new ArrayList<>(PARK_BATCH);
            synchronized (lock) {
                if (parked.isEmpty()) {
                    parkedBytes = 0;
                    return;
                }
                for (int i = 0; i < PARK_BATCH && !parked.isEmpty(); i++) {
                    byte[] f = parked.poll();
                    parkedBytes -= f.length;
                    batch.add(f);
                }
            }
            int written = 0;
            try {
                for (; written < batch.size(); written++) p.send(packet(batch.get(written)));
            } catch (IOException e) {
                synchronized (lock) {
                    for (int i = batch.size() - 1; i >= written; i--) {
                        parked.addFirst(batch.get(i));
                        parkedBytes += batch.get(i).length;
                    }
                }
                return;
            }
        }
    }

    public boolean drained() {
        synchronized (lock) {
            return queue.isEmpty() && parked.isEmpty() && unacked() == 0;
        }
    }

    public int parkedBytes() {
        synchronized (lock) { return parkedBytes; }
    }

    public java.util.Set<Long> protectedSeqs() {
        synchronized (lock) { return new java.util.HashSet<>(nackWanted.keySet()); }
    }

    private void bufferFec(long seq, byte[] payload) {
        synchronized (fecLock) {
            if (fecPending.isEmpty()) {
                fecBlockBase = seq;
                fecBlockStartMs = System.currentTimeMillis();
            }
            fecPending.add(payload);
            if (fecPending.size() == fecK) flushFecBlock(fecK);
        }
    }

    private void maybeFlushFec() {
        if (!fecEnabled) return;
        synchronized (fecLock) {
            if (!fecPending.isEmpty() && fecPending.size() < fecK
                    && System.currentTimeMillis() - fecBlockStartMs >= FEC_FLUSH_MS) {
                flushFecBlock(fecPending.size());
            }
        }
    }

    private void flushFecBlock(int m) {
        Path p = route.path();
        if (p == null || !p.usable()) {
            synchronized (fecLock) { fecPending.clear(); fecBlockBase = -1; fecBlockStartMs = 0; }
            return;
        }
        int K = fecK, R = fecR;
        byte[][] src = new byte[K][Mux.FRAG_SIZE];
        int[] lens = new int[K];
        for (int i = 0; i < m; i++) {
            byte[] pay = fecPending.poll();
            if (pay == null) break;
            System.arraycopy(pay, 0, src[i], 0, pay.length);
            lens[i] = pay.length;
        }
        byte[][] parity = Fec.encode(src, K, R);
        long blockId = fecBlockBase / K;
        for (int i = 0; i < R; i++) {
            byte[] body = buildFecBody(blockId, i, K, R, m, lens, parity[i]);
            try {
                p.send(Pack.build(connId, Pack.KIND_FEC, body, 0, body.length));
            } catch (IOException ignored) {
            }
        }
        synchronized (fecLock) { fecBlockBase = -1; fecBlockStartMs = 0; }
    }

    private static byte[] buildFecBody(long blockId, int parityIndex, int K, int R, int kactual, int[] lens, byte[] parity) {
        byte[] body = new byte[12 + 2 * K + parity.length];
        for (int i = 0; i < 8; i++) body[i] = (byte) (blockId >>> (8 * (7 - i)));
        body[8] = (byte) parityIndex;
        body[9] = (byte) K;
        body[10] = (byte) R;
        body[11] = (byte) kactual;
        int o = 12;
        for (int j = 0; j < K; j++) {
            body[o++] = (byte) (lens[j] >>> 8);
            body[o++] = (byte) lens[j];
        }
        System.arraycopy(parity, 0, body, o, parity.length);
        return body;
    }

    public void close() {
        if (fecEnabled) {
            synchronized (fecLock) {
                if (!fecPending.isEmpty()) flushFecBlock(fecPending.size());
            }
        }
        closed = true;
        flusher.interrupt();
        queue.clear();
        synchronized (lock) {
            inflight.clear();
            nackWanted.clear();
            parked.clear();
            parkedBytes = 0;
        }
    }

    private byte[] packet(byte[] frame) {
        return Pack.build(connId, Pack.KIND_DATA, frame, 0, frame.length);
    }

    private void offer(byte[] frame) throws IOException {
        long deadline = System.currentTimeMillis() + COMMIT_WINDOW_WAIT_MS;
        while (true) {
            if (closed) throw new IOException("sender closed");
            try {
                if (queue.offer(frame, 200, java.util.concurrent.TimeUnit.MILLISECONDS)) return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("sender closed: interrupted");
            }
            if (System.currentTimeMillis() >= deadline) {
                closed = true;
                flusher.interrupt();
                Trace.warn("[P2P] Outbound queue not drained within {}ms ({} frames backlogged), closing the connection to avoid byte-stream misalignment",
                        COMMIT_WINDOW_WAIT_MS, queue.size());
                throw new IOException("sender closed: outbound queue full for too long");
            }
        }
    }

    private void flushLoop() {
        long backoff = 0;
        while (!closed) {
            byte[] frame;
            try {
                frame = queue.take();
            } catch (InterruptedException e) {
                return;
            }
            try {

                if (!isControl(frame)) pace();
                if (transmit(frame)) {
                    backoff = 0;
                    synchronized (lock) {
                        framesSent++;
                        lossWinSent++;
                    }
                } else {
                    queue.offerFirst(frame);
                    backoff = Math.min(500, Math.max(50, backoff + 50));
                    sleepQuietly(backoff);
                }
            } catch (Throwable t) {
                queue.offerFirst(frame);
                sleepQuietly(50);
            }
        }
    }

    private boolean transmit(byte[] frame) throws IOException {
        Path p = route.path();
        if (p != null && p.usable()) {
            p.send(packet(frame));
            return true;
        }
        synchronized (lock) {
            if (parkedBytes + frame.length > PARK_MAX_BYTES) {
                Trace.periodic("[P2P] No usable path and the holding area is full ({}B), pausing new intake", parkedBytes);
                return false;
            }
            parkedBytes += frame.length;
            parked.add(frame);
        }
        return true;
    }

    private void trim() {
        Iterator<Long> it = inflight.keySet().iterator();
        while (inflight.size() > SEND_WINDOW && it.hasNext()) {
            long s = it.next();
            if (s <= lastAcked) it.remove();
            else break;
        }
        if (inflight.size() > ABSOLUTE_MAX_FRAMES) {
            int need = inflight.size() - ABSOLUTE_MAX_FRAMES;
            int dropped = 0;
            Iterator<Long> it2 = inflight.keySet().iterator();
            while (need > 0 && it2.hasNext()) {
                long s = it2.next();
                if (s <= lastAcked) continue;
                if (nackWanted.containsKey(s)) continue;
                it2.remove();
                need--;
                dropped++;
            }
            if (dropped > 0) {
                Trace.warn("[P2P] Unacknowledged fragments exceeded the hard cap, evicted {} ({} NACKed fragments protected)",
                        dropped, nackWanted.size());
            }
        }
        expireNackWanted();
    }

    private void expireNackWanted() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<Long, Long>> it = nackWanted.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            if (e.getKey() <= lastAcked || now - e.getValue() > NACK_TTL_MS) it.remove();
        }
        while (nackWanted.size() > MAX_NACK_WANTED) {
            Iterator<Long> k = nackWanted.keySet().iterator();
            k.next();
            k.remove();
        }
    }

    private static long rtoFor(Path p) {
        int ping = p.rtt();
        if (ping <= 0) return 1_000;
        return Math.min(RTO_CEIL_MS, Math.max(RTO_FLOOR_MS, ping * 2L + 100));
    }

    private static long backoff(long base, int retries) {
        int e = Math.min(retries, 5);
        double f = 1.0;
        for (int i = 0; i < e; i++) f *= 1.5;
        return (long) (base * f);
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
