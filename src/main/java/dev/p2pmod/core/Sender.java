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

    /** Free delivery slots the peer last advertised; -1 means "it never said". */
    private volatile int peerCredit = -1;
    private long creditBlockedSince;
    private static final long CREDIT_BLOCK_MAX_MS = 120_000;

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
        /**
         * 0 means the frame is still sitting in the outbound queue and has never hit the
         * socket. It must not be judged by the retransmission timer, otherwise a frame
         * that is merely queued behind a slow link looks like a loss, gets sent twice
         * (once by the retransmit below, once by the flusher when it finally pops the
         * queue) and pollutes the loss statistics.
         */
        long sentAt;
        int retries;
        Record(byte[] frame) { this.frame = frame; }
    }

    public Sender(int connId, Route route) {
        this.connId = connId;
        this.route = route;
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
            off += n;
            committed = true;
        }
    }

    private void reserve(boolean committed) throws IOException {
        long budget = committed ? COMMIT_WINDOW_WAIT_MS : OFFER_TIMEOUT_MS;
        long deadline = System.currentTimeMillis() + budget;
        while (true) {
            // The peer told us its delivery buffer is full. Blocking the caller (the
            // local socket reader) is the correct backpressure: it shrinks the TCP
            // window towards Minecraft instead of piling up in the tunnel. This is not
            // a dead link, so it must not trip the window-full timeout below.
            if (peerCredit == 0 && ackSilenceMs() <= 5_000) {
                long now = System.currentTimeMillis();
                if (creditBlockedSince == 0) creditBlockedSince = now;
                if (now - creditBlockedSince > CREDIT_BLOCK_MAX_MS) {
                    closed = true;
                    flusher.interrupt();
                    Trace.warn("[P2P] conn={} peer reported a full delivery buffer for {}ms, closing the connection",
                            connId, now - creditBlockedSince);
                    throw new IOException("sender closed: peer delivery buffer never drained");
                }
                if (closed) throw new IOException("sender closed");
                sleepQuietly(10);
                continue;
            }
            creditBlockedSince = 0;
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
                if (r.sentAt == 0) continue;
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
                if (r.sentAt == 0) continue;
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

    /**
     * Ceiling for the in-flight window, derived from the measured throughput and RTT.
     * Without it the window grows to SEND_WINDOW no matter how little the link can
     * carry, and everything beyond one bandwidth-delay product simply piles up in the
     * path and gets dropped. On the observed ~250 KB/s link with 50 ms RTT the usable
     * BDP is about 10 fragments, against a ceiling of 8192.
     */
    private int windowLimit() {
        int rate = ackedRate;
        if (rate <= 0) return SEND_WINDOW;
        Path p = route.path();
        int rtt = (p != null && p.rtt() > 0) ? p.rtt() : 100;
        long bdp = (long) rate * rtt / 1000L;
        long cap = bdp * 3 + 16;
        return (int) Math.max(64, Math.min(SEND_WINDOW, cap));
    }

    private void growCwnd(long now) {
        if (now - lastCwndGrowMs < CWND_GROW_GAP_MS) return;
        lastCwndGrowMs = now;
        int c = cwnd;
        int limit = windowLimit();
        if (c > limit) {
            // The measured capacity is below the current window: ease back towards it
            // instead of keeping a window that only feeds the bottleneck queues.
            cwnd = Math.max(limit, c - Math.max(CWND_GROW_MIN, c / 8));
            return;
        }
        if (c >= limit) return;
        cwnd = Math.min(limit, c + Math.max(CWND_GROW_MIN, c / 8));
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

        // Floor of 64 fragments/s (~83 KB/s). The old floor of 1500 fragments/s
        // (~1.95 MB/s) was several times faster than the links this runs on, so pacing
        // never actually throttled anything.
        if (rate < 64) rate = 64;

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
                if (r.sentAt == 0) continue;
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

    public void emitAck(long cumulative, int credit) {
        if (closed) return;
        queue.offer(credit >= 0
                ? Mux.controlWithCredit(Mux.CTRL_ACK, cumulative, credit)
                : Mux.control(Mux.CTRL_ACK, cumulative));
    }

    /** Free delivery slots the peer advertised; -1 when it does not report any. */
    public void onCredit(int credit) {
        if (credit >= 0) peerCredit = credit;
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
                for (; written < batch.size(); written++) {
                    p.send(packet(batch.get(written)));
                    markSent(batch.get(written));
                }
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

    public void close() {
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

    /**
     * Stamps a record only once the socket has actually taken the frame. Frames still
     * queued (or parked waiting for a path) keep sentAt == 0, so the retransmission
     * timers skip them: they were never lost, they simply have not been sent yet.
     */
    private void markSent(byte[] frame) {
        if (isControl(frame)) return;
        long seq = Mux.seqOf(frame, 0);
        synchronized (lock) {
            Record r = inflight.get(seq);
            if (r != null && r.sentAt == 0) r.sentAt = System.currentTimeMillis();
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
            markSent(frame);
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
            // Evicting unacknowledged frames punches holes into the byte stream: the peer
            // then parses garbage. Failing the connection is the safe outcome, because
            // Minecraft simply reconnects and the stream restarts cleanly.
            Trace.error("[P2P] conn={} unacknowledged fragments exceeded the hard cap ({}), closing the "
                    + "connection instead of silently dropping frames", connId, inflight.size());
            closed = true;
            flusher.interrupt();
            inflight.clear();
            nackWanted.clear();
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
