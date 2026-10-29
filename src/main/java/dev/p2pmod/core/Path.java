package dev.p2pmod.core;

import java.io.IOException;
import java.net.InetSocketAddress;

public final class Path {

    public enum Via { RELAY, DIRECT }

    public enum State { PROBING, LIVE, DEAD }

    public interface Sink { void send(byte[] packet) throws IOException; }

    private static final long DEAD_AFTER_MS = 15_000;

    private final Via via;
    private final Sink sink;
    private volatile InetSocketAddress remote;
    private volatile String label;

    private volatile State state = State.PROBING;
    private volatile long lastInMs;
    private volatile long lastAppDataMs;
    private volatile long createdMs = System.currentTimeMillis();

    private volatile int rtt = -1;
    private volatile int lastRtt = -1;
    private volatile int rttEwma = -1;

    private volatile long pendingNonce;
    private volatile long challengeSentMs;
    private volatile int failedChallenges;

    private volatile long stallUntilMs;

    public void markDataStall(long holdMs) {
        stallUntilMs = System.currentTimeMillis() + Math.max(1_000, holdMs);
    }

    public boolean dataStalled() {
        return System.currentTimeMillis() < stallUntilMs;
    }

    public Path(Via via, Sink sink, InetSocketAddress remote, String label) {
        this.via = via;
        this.sink = sink;
        this.remote = remote;
        this.label = label;
    }

    public Via via() { return via; }

    public String label() { return label; }

    public InetSocketAddress remote() { return remote; }

    public void rebase(InetSocketAddress actual) {
        if (actual != null && !actual.equals(remote)) {
            remote = actual;
        }
    }

    public void send(byte[] packet) throws IOException {
        sink.send(packet);
    }

    public State state() { return state; }

    public boolean usable() { return state == State.LIVE; }

    public void noteInbound(boolean appData) {
        long now = System.currentTimeMillis();
        lastInMs = now;
        if (appData) lastAppDataMs = now;
    }

    public void promote() {
        if (state != State.DEAD) state = State.LIVE;
    }

    public long lastAppDataMs() { return lastAppDataMs; }

    public long lastInMs() { return lastInMs; }

    public long createdMs() { return createdMs; }

    public void revive() {
        pendingNonce = 0;
        failedChallenges = 0;
        lastInMs = 0;
        lastAppDataMs = 0;
        rtt = -1;
        lastRtt = -1;
        rttEwma = -1;
        createdMs = System.currentTimeMillis();
        state = State.PROBING;
    }

    public long inboundSilenceMs() {
        long ref = lastAppDataMs > 0 ? lastAppDataMs : createdMs;
        return System.currentTimeMillis() - ref;
    }

    public boolean expired() {
        long now = System.currentTimeMillis();
        long ref = lastInMs > 0 ? lastInMs : createdMs;
        return now - ref > DEAD_AFTER_MS;
    }

    public void markDead() { state = State.DEAD; }

    public void armChallenge(long nonce, long now) {
        pendingNonce = nonce;
        challengeSentMs = now;
    }

    public long pendingNonce() { return pendingNonce; }

    public boolean verifyChallenge(long nonce) {
        if (pendingNonce != 0 && pendingNonce == nonce) {
            pendingNonce = 0;
            failedChallenges = 0;

            long spent = System.currentTimeMillis() - challengeSentMs;
            if (spent > 0 && spent < 5_000) noteRtt((int) spent);
            promote();
            return true;
        }
        return false;
    }

    public void noteChallengeTimeout(long expiredBeforeMs) {
        if (pendingNonce != 0 && challengeSentMs > 0 && challengeSentMs < expiredBeforeMs) {
            pendingNonce = 0;
            if (++failedChallenges >= 3 && state == State.LIVE) {
                state = State.PROBING;
            }
        }
    }

    public int failedChallenges() { return failedChallenges; }

    public int rtt() { return rtt; }

    public int lastRtt() { return lastRtt; }

    public void noteRtt(int sample) {
        if (sample <= 0) return;
        lastRtt = sample;
        rttEwma = rttEwma <= 0 ? sample : (rttEwma * 7 + sample * 3) / 10;
        rtt = rttEwma;
    }

    public int score() {
        if (!usable()) return -1;

        long base = rtt > 0 ? rtt : 500;
        if (via == Via.RELAY) base += 30;
        long ref = lastInMs > 0 ? lastInMs : createdMs;
        long silence = Math.min(Math.max(0, System.currentTimeMillis() - ref), 5_000);
        long s = base + silence / 100;

        if (dataStalled()) s += 5_000;
        return (int) s;
    }

    @Override
    public String toString() {
        return label + "{" + via + ", " + remote + ", " + state + ", rtt=" + rtt + "ms}";
    }
}
