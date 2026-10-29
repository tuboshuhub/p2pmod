package dev.p2pmod.core;

import dev.p2pmod.support.Scheduler;
import dev.p2pmod.support.Trace;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Connection implements Endpoint.ConnObserver {

    private static final long SWITCH_COOLDOWN_MS = 5_000;

    private static final int SWITCH_MARGIN = 25;

    private static final long PROBE_INTERVAL_MS = 1_000;

    private static final long FAST_PROBE_INTERVAL_MS = 250;
    private static final long FAST_PROBE_WINDOW_MS = 3_000;

    private static final long PING_INTERVAL_MS = 1_000;
    private static final long ACK_HEARTBEAT_MS = 1_000;

    public static final long STALL_MS = 15_000;

    private static final long ACK_STALL_MS = 3_000;

    private static final long WINDOW_FULL_MS = 5_000;

    private static final int BACKLOG_HIGH = 4_096;
    private static final int BACKLOG_LOW = 1_536;
    private static final long BACKLOG_STALL_MS = 8_000;

    private static final long OUT_STUCK_WARN_MS = 2_000;

    private static final long FIRST_PUNCH_WINDOW_MS = 15_000;

    private long backlogHighSinceMs;

    private static final long STALL_COOLDOWN_MS = 8_000;

    private static final long STALL_HOLD_MS = 60_000;

    public interface SwitchHook { void onSwitch(Path path, boolean initial); }

    private final int connId;
    private final Endpoint endpoint;
    private final Receiver receiver;
    private final Sender sender;
    private final Map<String, Path> paths = new ConcurrentHashMap<>();

    private volatile Path preferred;
    private volatile SwitchHook hook = (p, initial) -> {};
    private volatile boolean closed;

    private volatile boolean allowDirect = true;

    public void setAllowDirect(boolean v) {
        this.allowDirect = v;
    }
    private volatile boolean peerClosed;

    private volatile boolean preferRelay;
    private volatile boolean peerPreferRelay;

    private static final long RELAY_SILENT_MS = 3_000;
    private volatile long lastModeSentMs;

    private volatile int peerLossTenths;

    private volatile int peerToken = -1;

    private static final long ANNOUNCE_WINDOW_MS = 20_000;

    private static final long ANNOUNCE_FAST_INTERVAL_MS = 200;
    private static final long ANNOUNCE_FAST_MS = 2_000;
    private static final long ANNOUNCE_INTERVAL_MS = 800;

    private volatile InetSocketAddress lastExit;

    private volatile java.util.List<java.net.InetAddress> localV6 = java.util.List.of();

    private static final int MAX_DIRECT_CANDIDATES = 3;

    private volatile long announceUntilMs;
    private volatile long announceStartMs;
    private volatile long lastAnnounceMs;

    private long lastSwitchMs;
    private long lastProbeMs;
    private long lastFastProbeMs;
    private long lastPingMs;
    private long lastAckBeatMs;
    private long lastMoveMs = System.currentTimeMillis();
    private long lastDiagMs;

    private long lastLossWarnMs;

    private long lastLossSampleMs;

    private long outStuckSinceMs;
    private long lastStallActionMs;

    private final SecureRandom rng = new SecureRandom();
    private final Runnable beat = this::beatSafely;
    private final long createdAtMs = System.currentTimeMillis();

    public Connection(int connId, Endpoint endpoint, Receiver.AppSink app) {
        this.connId = connId;
        this.endpoint = endpoint;
        this.receiver = new Receiver(

                new Receiver.ControlSink() {
                    @Override public void onAck(long cumulative) {
                        sender.onAck(cumulative);
                        lastMoveMs = System.currentTimeMillis();
                    }
                    @Override public void onNack(long missing) { sender.onNack(missing); }
                },

                new Receiver.Feedback() {
                    @Override public void ack(long cumulative) { sender.emitAck(cumulative); }
                    @Override public void nack(long missing) { sender.emitNack(missing); }
                },
                data -> {
                    lastMoveMs = System.currentTimeMillis();
                    app.accept(data);
                });
        this.sender = new Sender(connId, () -> preferred);
        endpoint.bind(connId, this);
        Scheduler.register(beat);
    }

    public int connId() { return connId; }

    public void setSwitchHook(SwitchHook h) { this.hook = (h == null) ? ((p, initial) -> {}) : h; }

    public Path addRelay(InetSocketAddress relayAddr, int myToken, int peerToken) {
        String key = "relay";
        this.peerToken = peerToken;
        Path existing = paths.get(key);
        if (existing != null) return existing;
        Path p = new Path(Path.Via.RELAY,
                b -> endpoint.send(relayAddr, Signaling.forward(myToken, peerToken, b)),
                relayAddr, "relay");
        p.promote();
        paths.put(key, p);

        reselect(now());

        measure();

        Trace.debug("[P2P] Relay path ready {} (peerToken={})", relayAddr, peerToken);
        return p;
    }

    private static long now() { return System.currentTimeMillis(); }

    private void pruneDirectCandidates() {
        int direct = 0;
        for (Path p : paths.values()) if (p.via() == Path.Via.DIRECT) direct++;
        if (direct < MAX_DIRECT_CANDIDATES) return;
        String victimKey = null;
        for (Map.Entry<String, Path> e : paths.entrySet()) {
            Path p = e.getValue();
            if (p.via() != Path.Via.DIRECT || p.usable()) continue;
            if (p.state() == Path.State.DEAD) {
                victimKey = e.getKey();
                break;
            }
            if (victimKey == null) victimKey = e.getKey();
        }
        if (victimKey == null) return;
        Trace.debug("[P2P] Dropping stale direct candidate {}", victimKey);
        paths.remove(victimKey);
    }

    public Path probeDirect(InetSocketAddress peer) {
        if (peer == null || !allowDirect) return null;

        java.net.InetAddress real = Bytes.normalize(peer);
        if (real != null && !real.equals(peer.getAddress())) {
            peer = new InetSocketAddress(real, peer.getPort());
        }
        if (!punchable(peer)) {

            Trace.debug("[P2P] No direct candidate from the relay's view: peer address {} is not punchable "
                    + "(a relay behind a tunnel cannot see the real egress); waiting for the in-band announcement", peer);
            return null;
        }
        String key = "direct:" + peer;

        Path existing = paths.get(key);
        if (existing != null) {
            if (existing.state() == Path.State.DEAD) existing.revive();
            return existing;
        }
        pruneDirectCandidates();

        final Path[] holder = new Path[1];
        Path p = new Path(Path.Via.DIRECT, b -> endpoint.send(holder[0].remote(), b), peer, "direct");
        holder[0] = p;
        paths.put(key, p);
        Trace.debug("[P2P] Probing direct path {}", peer);

        challenge(p);
        return p;
    }

    public static boolean punchable(InetSocketAddress addr) {
        if (addr == null || addr.isUnresolved()) return false;
        java.net.InetAddress a = addr.getAddress();
        if (a == null) return false;
        return !a.isLoopbackAddress() && !a.isAnyLocalAddress()
                && !a.isLinkLocalAddress() && !a.isMulticastAddress();
    }

    public Path preferred() { return preferred; }

    public Map<String, Path> paths() { return paths; }

    @Override
    public boolean acceptsRelayFrom(int fromToken) {
        int expected = peerToken;

        return expected > 0 && fromToken == expected;
    }

    @Override
    public void onPacket(int cid, byte kind, byte[] body, InetSocketAddress from) {
        if (closed || cid != connId) return;
        Path p = resolvePath(from, kind);
        if (p == null) return;
        p.noteInbound(kind == Pack.KIND_DATA);

        switch (kind) {
            case Pack.KIND_DATA -> receiver.ingest(body);

            case Pack.KIND_PING -> {
                lastMoveMs = System.currentTimeMillis();
                if (body.length >= 8) {
                    if (body.length >= 12) peerLossTenths = Bytes.getInt(body, 8);
                    byte[] pong = new byte[12];
                    Bytes.putLong(pong, 0, Bytes.getLong(body, 0));
                    Bytes.putInt(pong, 8, Math.max(0, p.rtt()));
                    reply(p, Pack.KIND_PONG, pong);
                }
            }
            case Pack.KIND_PONG -> {
                lastMoveMs = System.currentTimeMillis();
                if (body.length >= 8) {
                    long echoed = Bytes.getLong(body, 0);
                    int sample = (int) (System.currentTimeMillis() - echoed);
                    int peerSeen = body.length >= 12 ? Bytes.getInt(body, 8) : 0;

                    if (peerSeen > 0 && sample * 2 < peerSeen) sample = peerSeen;
                    p.noteRtt(sample);
                }
            }
            case Pack.KIND_CHALLENGE -> {

                if (allowDirect && body.length >= 8) reply(p, Pack.KIND_RESPONSE, body);
            }
            case Pack.KIND_RESPONSE -> {
                if (body.length >= 8 && p.verifyChallenge(Bytes.getLong(body, 0))) {
                    Trace.info("[P2P] {} verified, now usable", p.label());
                }
            }
            case Pack.KIND_NATADDR -> {
                if (allowDirect && body.length >= Bytes.ADDR_LEN) {
                    InetSocketAddress a = Bytes.getAddr(body, 0);
                    if (a != null && punchable(a)) {

                    Trace.periodic("[P2P] Peer reported real public egress {}, attempting direct", a);
                        probeDirect(a);

                        if (lastExit != null && now() - lastAnnounceMs >= ANNOUNCE_INTERVAL_MS) {
                            sendExit();
                        }
                    }
                }
            }
            case Pack.KIND_MODE -> {
                if (body.length >= 1) {
                    boolean v = body[0] != 0;
                    if (v != peerPreferRelay) {
                        peerPreferRelay = v;
                        Trace.info("[P2P] conn={} peer asked to {} the relay", connId, v ? "prefer" : "stop preferring");
                        reselect(now());
                    }
                }
            }
            case Pack.KIND_CLOSE -> {

                peerClosed = true;
                for (Path q : paths.values()) q.markDead();
                preferred = null;
            }
            default -> { }
        }
    }

    public void announceExit(InetSocketAddress mapped) {
        if (mapped == null || closed) return;
        lastExit = mapped;
        armAnnounce();
        sendExit();
    }

    public void announceLocalV6(boolean peerIsNear) {
        if (closed) return;
        java.util.List<java.net.InetAddress> found = localV6Addresses(peerIsNear);
        if (found.isEmpty()) return;
        localV6 = found;
        Trace.debug("[P2P] conn={} announcing {} local IPv6 address(es)", connId, found.size());
        armAnnounce();
        sendExit();
    }

    public static java.util.List<java.net.InetAddress> localV6Addresses(boolean includeUla) {
        long now = System.currentTimeMillis();
        java.util.List<java.net.InetAddress> cached = V6_CACHE;
        if (cached != null && now - v6CacheMs < 60_000) {
            return includeUla ? cached : globalOnly(cached);
        }
        java.util.List<java.net.InetAddress> out = new java.util.ArrayList<>(2);
        java.util.List<java.net.InetAddress> ula = new java.util.ArrayList<>(1);
        try {
            for (java.net.NetworkInterface ni
                    : java.util.Collections.list(java.net.NetworkInterface.getNetworkInterfaces())) {
                if (!ni.isUp() || ni.isLoopback()) continue;
                for (java.net.InetAddress a : java.util.Collections.list(ni.getInetAddresses())) {
                    if (!(a instanceof java.net.Inet6Address)) continue;
                    if (a.isLoopbackAddress() || a.isLinkLocalAddress() || a.isSiteLocalAddress()
                            || a.isMulticastAddress() || a.isAnyLocalAddress()) continue;

                    if (a.getAddress()[0] == (byte) 0xFD || a.getAddress()[0] == (byte) 0xFC) {
                        ula.add(a);
                    } else {
                        out.add(a);
                    }
                }
            }
        } catch (Exception ignored) {
        }

        out.addAll(ula);
        if (out.size() > 2) out = new java.util.ArrayList<>(out.subList(0, 2));
        V6_CACHE = out;
        v6CacheMs = now;
        return includeUla ? out : globalOnly(out);
    }

    private static java.util.List<java.net.InetAddress> globalOnly(java.util.List<java.net.InetAddress> all) {
        java.util.List<java.net.InetAddress> out = new java.util.ArrayList<>(all.size());
        for (java.net.InetAddress a : all) if (!Bytes.isPrivate(a)) out.add(a);
        return out;
    }

    private static volatile java.util.List<java.net.InetAddress> V6_CACHE;
    private static volatile long v6CacheMs;

    private java.util.List<InetSocketAddress> exitCandidates() {
        java.util.List<InetSocketAddress> out = new java.util.ArrayList<>(3);
        InetSocketAddress v4 = lastExit;
        if (v4 != null) out.add(v4);
        java.util.List<java.net.InetAddress> v6 = localV6;
        if (!v6.isEmpty()) {
            int port = endpoint == null ? 0 : endpoint.localPort();
            if (port > 0) {
                for (java.net.InetAddress a : v6) out.add(new InetSocketAddress(a, port));
            }
        }
        return out;
    }

    private void armAnnounce() {
        long now = now();
        announceUntilMs = now + ANNOUNCE_WINDOW_MS;
        announceStartMs = now;
    }

    private void sendExit() {
        if (closed) return;
        java.util.List<InetSocketAddress> exits = exitCandidates();
        if (exits.isEmpty()) return;
        for (Path p : paths.values()) {
            if (p.state() == Path.State.DEAD) continue;
            for (InetSocketAddress a : exits) {
                byte[] body = new byte[Bytes.ADDR_LEN];
                Bytes.putAddr(body, 0, a);
                for (int i = 0; i < 3; i++) reply(p, Pack.KIND_NATADDR, body);
            }
        }
        lastAnnounceMs = now();
    }

    private void repeatAnnounce(long now) {
        if (!allowDirect || (lastExit == null && localV6.isEmpty())) return;
        if (announceUntilMs == 0 || now > announceUntilMs) return;
        if (hasLiveDirect()) {

            announceUntilMs = 0;
            return;
        }
        long interval = now - announceStartMs < ANNOUNCE_FAST_MS
                ? ANNOUNCE_FAST_INTERVAL_MS : ANNOUNCE_INTERVAL_MS;
        if (now - lastAnnounceMs < interval) return;
        sendExit();
    }

    private boolean hasLiveDirect() {
        for (Path p : paths.values()) {
            if (p.via() == Path.Via.DIRECT && p.usable()) return true;
        }
        return false;
    }

    public void setPreferRelay(boolean v) {
        if (preferRelay == v) return;
        preferRelay = v;
        lastModeSentMs = 0;
        sendMode();
        reselect(now());
        Trace.info("[P2P] conn={} link policy -> {}", connId, v ? "prefer relay" : "auto");
    }

    public boolean preferRelay() {
        return preferRelay || peerPreferRelay;
    }

    private void sendMode() {
        byte[] body = new byte[] { (byte) (preferRelay ? 1 : 0) };
        for (Path p : paths.values()) {
            if (p.state() == Path.State.DEAD) continue;
            for (int i = 0; i < 3; i++) reply(p, Pack.KIND_MODE, body);
        }
        lastModeSentMs = now();
    }

    private void repeatMode(long now) {
        if (now - lastModeSentMs < 5_000) return;
        sendMode();
    }

    private boolean relayFresh(long now) {
        Path relay = paths.get("relay");
        if (relay == null) return false;
        return now - Math.max(relay.lastInMs(), relay.createdMs()) <= RELAY_SILENT_MS;
    }

    public boolean linkDead(long ms) {
        if (paths.isEmpty()) return true;
        long now = System.currentTimeMillis();
        for (Path p : paths.values()) {
            if (now - Math.max(p.lastInMs(), p.createdMs()) < ms) return false;
        }
        return true;
    }

    public record PathView(String label, boolean live, int rtt, long silentMs) {}

    public record LinkView(String route, int rtt, int upLossTenths, int downLossTenths,
                           PathView relay, PathView direct) {}

    public LinkView linkView() {
        Path cur = preferred;
        return new LinkView(cur == null ? "none" : cur.label(),
                cur == null ? -1 : displayRtt(cur),
                sender.lossTenths(), peerLossTenths,
                view(paths.get("relay")), view(directPath()));
    }

    private static int displayRtt(Path p) {
        int last = p.lastRtt();
        return last > 0 ? last : p.rtt();
    }

    private Path directPath() {
        for (Path p : paths.values()) {
            if (p.via() == Path.Via.DIRECT) return p;
        }
        return null;
    }

    private static PathView view(Path p) {
        if (p == null) return null;
        long now = System.currentTimeMillis();
        long silence = now - Math.max(p.lastInMs(), p.createdMs());
        return new PathView(p.label(), p.usable() && silence <= RELAY_SILENT_MS, displayRtt(p), silence);
    }

    private static boolean wentSilent(Path p, long now) {
        long last = p.lastInMs();
        return last > 0 && now - last > RELAY_SILENT_MS;
    }

    public String describe() {
        LinkView v = linkView();
        return String.format("link=%s(%dms) loss=%.1f%%/%.1f%% relay=%s direct=%s",
                v.route(), v.rtt(), v.upLossTenths() / 10.0, v.downLossTenths() / 10.0,
                sideSummary(v.relay()), sideSummary(v.direct()));
    }

    private static String sideSummary(PathView p) {
        if (p == null) return "none";
        if (!p.live()) return "DOWN";
        return p.rtt() > 0 ? p.rtt() + "ms" : "up";
    }

    private Path resolvePath(InetSocketAddress from, byte kind) {
        for (Path p : paths.values()) {
            if (p.remote() != null && p.remote().equals(from)) {

                if (p.state() == Path.State.DEAD) {
                    p.revive();
                    challenge(p);
                }
                return p;
            }
        }
        if (!allowDirect || kind == Pack.KIND_DATA || !punchable(from)) return null;

        Path candidate = null;
        for (Path p : paths.values()) {
            if (p.via() != Path.Via.DIRECT) continue;
            if (candidate == null || p.createdMs() > candidate.createdMs()) candidate = p;
        }
        if (candidate == null) {

            if (kind != Pack.KIND_CHALLENGE || now() - createdAtMs > FIRST_PUNCH_WINDOW_MS) return null;
            Path fresh = probeDirect(from);
            if (fresh == null) return null;
            Trace.info("[P2P] Peer punched first from {}; answering straight away instead of waiting for its announcement", from);
            return fresh;
        }

        Trace.info("[P2P] Rebinding direct candidate {} -> {} (peer's real egress, corrected from a punch packet)",
                candidate.remote(), from);
        candidate.revive();
        candidate.rebase(from);
        challenge(candidate);
        return candidate;
    }

    private void challenge(Path p) {
        try {
            long nonce = rng.nextLong();
            p.armChallenge(nonce, now());
            byte[] body = new byte[8];
            Bytes.putLong(body, 0, nonce);
            p.send(Pack.build(connId, Pack.KIND_CHALLENGE, body, 0, 8));
        } catch (IOException ignored) {
        }
    }

    private void reply(Path p, byte kind, byte[] body) {
        try {
            p.send(Pack.build(connId, kind, body, 0, body.length));
        } catch (IOException ignored) {
        }
    }

    private void beatSafely() {
        try { beat(); } catch (Throwable t) { Trace.debug("Connection heartbeat failed: {}", t.toString()); }
    }

    private void beat() {
        if (closed) return;
        long now = System.currentTimeMillis();

        reapPaths(now);
        repeatAnnounce(now);
        repeatMode(now);
        if (now - lastFastProbeMs >= FAST_PROBE_INTERVAL_MS) {
            lastFastProbeMs = now;
            probeYoungPaths(now);
        }
        if (now - lastProbeMs >= PROBE_INTERVAL_MS) {
            lastProbeMs = now;
            probePendingPaths(now);
        }
        if (now - lastPingMs >= PING_INTERVAL_MS) {
            lastPingMs = now;
            measure();
        }
        if (now - lastAckBeatMs >= ACK_HEARTBEAT_MS) {
            lastAckBeatMs = now;

            long cum = receiver.cumulative();
            if (cum >= 0) {
                byte[] ack = Mux.control(Mux.CTRL_ACK, cum);
                for (Path p : paths.values()) {
                    if (!p.usable()) continue;
                    try {
                        p.send(Pack.build(connId, Pack.KIND_DATA, ack, 0, ack.length));
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        reselect(now);
        sender.tick();
        receiver.pokeNack();
        updateBacklog(now);
        stallGuard(now);
        diag(now);
    }

    private void updateBacklog(long now) {
        int un = sender.unacked();

        int c = Math.max(64, sender.cwnd());
        int high = Math.min(BACKLOG_HIGH, c * 3 / 4);
        int low = Math.min(BACKLOG_LOW, Math.max(16, c / 4));
        if (un >= high) {
            if (backlogHighSinceMs == 0) backlogHighSinceMs = now;
        } else if (un <= low) {
            backlogHighSinceMs = 0;
        }
    }

    private void stallGuard(long now) {
        if (sender.unacked() <= 0) return;

        boolean ackStalled = sender.ackSilenceMs() >= ACK_STALL_MS;
        long fullMs = sender.windowFullMs();
        boolean saturated = fullMs >= WINDOW_FULL_MS;
        boolean backlogged = backlogHighSinceMs != 0
                && now - backlogHighSinceMs >= BACKLOG_STALL_MS;
        if (!ackStalled && !saturated && !backlogged) return;
        if (now - lastStallActionMs < STALL_COOLDOWN_MS) return;
        Path cur = preferred;
        if (cur == null) return;
        Path alt = null;
        for (Path p : paths.values()) {
            if (p == cur || !p.usable() || p.dataStalled()) continue;
            if (alt == null || p.score() < alt.score()) alt = p;
        }
        if (alt == null) return;
        lastStallActionMs = now;
        cur.markDataStall(STALL_HOLD_MS);
        preferred = alt;
        lastSwitchMs = now;
        if (ackStalled) {
            Trace.warn("[P2P] conn={} {} data plane stalled for {}ms ({} fragments unacked), using {} for now",
                    connId, cur.label(), sender.ackSilenceMs(), sender.unacked(), alt.label());
        } else if (backlogged) {
            Trace.warn("[P2P] conn={} {} backlog not drained for {}ms ({} fragments unacked, retransmissions ate the bandwidth), using {}",
                    connId, cur.label(), now - backlogHighSinceMs, sender.unacked(), alt.label());
        } else {
            Trace.warn("[P2P] conn={} {} window full for {}ms ({} fragments unacked), using {}",
                    connId, cur.label(), fullMs, sender.unacked(), alt.label());
        }
        sender.onPathChanged();
        sender.releaseParked();
        hook.onSwitch(alt, false);
    }

    private void diag(long now) {
        sampleLoss(now);
        reportHighLoss(now);
        int unacked = sender.unacked();
        long rcvStall = receiver.progressAgeMs();

        boolean outStuck = unacked > 0 && (
                sender.ackSilenceMs() >= ACK_STALL_MS / 3
                        || sender.windowFullMs() >= WINDOW_FULL_MS / 5
                        || backlogHighSinceMs != 0);
        boolean inStuck = rcvStall > 5_000 && !receiver.drained();
        if (!outStuck && !inStuck) {
            outStuckSinceMs = 0;
            return;
        }
        if (outStuck) {
            if (outStuckSinceMs == 0) outStuckSinceMs = now;

            if (now - outStuckSinceMs < OUT_STUCK_WARN_MS) return;
        } else {
            outStuckSinceMs = 0;
        }
        if (now - lastDiagMs < 5_000) return;
        lastDiagMs = now;
        StringBuilder sb = new StringBuilder();
        for (Path p : paths.values()) {
            sb.append(p.label()).append('[').append(p.state())
                    .append(" rtt=").append(p.rtt())
                    .append(" inboundSilence=").append(p.inboundSilenceMs()).append("ms] ");
        }
        Trace.warn("[P2P] conn={} {} fragments unacked (cwnd={}, loss {}%, ACK silence {}ms, window full {}ms) preferred={}; recv[{} stuck {}ms]; {}",
                connId, unacked, sender.cwnd(), sender.lossPercent(),
                sender.ackSilenceMs(), sender.windowFullMs(),
                preferred == null ? "none" : preferred.label(),
                receiver.stats(), rcvStall, sb.toString().trim());
    }

    private void sampleLoss(long now) {
        if (now - lastLossSampleMs < 30_000) return;
        lastLossSampleMs = now;
        Trace.debug("[P2P] conn={} link quality: up={}% down={}% (cwnd={}, unacked={}, preferred={})",
                connId, String.format(java.util.Locale.ROOT, "%.1f", sender.lossTenths() / 10.0),
                String.format(java.util.Locale.ROOT, "%.1f", peerLossTenths / 10.0),
                sender.cwnd(), sender.unacked(), preferred == null ? "none" : preferred.label());
    }

    private void reportHighLoss(long now) {
        int up = sender.lossTenths();
        int down = peerLossTenths;
        if (up < 50 && down < 50) return;
        if (now - lastLossWarnMs < 10_000) return;
        lastLossWarnMs = now;
        Trace.warn("[P2P] conn={} high loss: up={}% down={}% (cwnd={}, unacked={}, {} unacked-bytes, preferred={})",
                connId, String.format(java.util.Locale.ROOT, "%.1f", up / 10.0),
                String.format(java.util.Locale.ROOT, "%.1f", down / 10.0),
                sender.cwnd(), sender.unacked(), sender.unacked() * 1300,
                preferred == null ? "none" : preferred.label());
    }

    private void reapPaths(long now) {
        for (Map.Entry<String, Path> e : paths.entrySet()) {
            Path p = e.getValue();
            if (p.state() == Path.State.DEAD) continue;
            p.noteChallengeTimeout(now - PROBE_INTERVAL_MS * 2);
            if (p.via() == Path.Via.RELAY) continue;
            if (p.expired()) {
                p.markDead();

                if (lastExit != null || !localV6.isEmpty()) armAnnounce();

                if (sender.unacked() == 0 && receiver.drained()) {
                    Trace.debug("[P2P] conn={} {} expired while idle (peer stopped), dropping it from selection",
                            connId, p.label());
                } else {
                    Trace.warn("[P2P] conn={} {} silent for too long, marked unusable (relay usable={})",
                            connId, p.label(), relayAlive());
                }
            }
        }
    }

    private boolean relayAlive() {
        Path r = paths.get("relay");
        return r != null && r.usable();
    }

    private void probeYoungPaths(long now) {
        for (Path p : paths.values()) {
            if (p.via() != Path.Via.DIRECT || p.state() != Path.State.PROBING) continue;
            if (now - p.createdMs() > FAST_PROBE_WINDOW_MS) continue;
            long nonce = p.pendingNonce();
            if (nonce == 0) {
                nonce = rng.nextLong();
                p.armChallenge(nonce, now);
            }
            byte[] body = new byte[8];
            Bytes.putLong(body, 0, nonce);
            try {
                p.send(Pack.build(connId, Pack.KIND_CHALLENGE, body, 0, 8));
            } catch (IOException ignored) {
            }
        }
    }

    private void probePendingPaths(long now) {
        for (Path p : paths.values()) {
            if (p.state() != Path.State.PROBING || p.via() != Path.Via.DIRECT) continue;
            if (p.pendingNonce() != 0) continue;
            long nonce = rng.nextLong();
            p.armChallenge(nonce, now);
            try {
                byte[] body = new byte[8];
                Bytes.putLong(body, 0, nonce);
                p.send(Pack.build(connId, Pack.KIND_CHALLENGE, body, 0, 8));
            } catch (IOException ignored) {
            }
        }
    }

    private void measure() {
        for (Path p : paths.values()) {
            if (p.state() == Path.State.DEAD) continue;

            byte[] body = new byte[12];
            Bytes.putLong(body, 0, System.currentTimeMillis());
            Bytes.putInt(body, 8, sender.lossTenths());
            try {
                p.send(Pack.build(connId, Pack.KIND_PING, body, 0, 12));
            } catch (IOException ignored) {
            }
        }
    }

    public static Path applyPolicy(Path scored, Path relay, boolean relayFirst, boolean relayAnswering) {
        return (relayFirst && relayAnswering && relay != null) ? relay : scored;
    }

    private void reselect(long now) {
        Path best = null;
        for (Path p : paths.values()) {
            if (!p.usable()) continue;

            if (wentSilent(p, now)) continue;
            int s = p.score();
            if (s < 0) continue;
            if (best == null || s < best.score()) best = p;
        }
        if (best == null) return;

        boolean relayFirst = preferRelay();
        Path relay = paths.get("relay");
        best = applyPolicy(best, relay, relayFirst, relay != null && relay.usable() && relayFresh(now));

        Path cur = preferred;
        if (best == cur) return;

        boolean curDead = cur == null || !cur.usable() || wentSilent(cur, now);

        if (!curDead && now - lastSwitchMs < SWITCH_COOLDOWN_MS) return;

        if (!relayFirst) {
            int curScore = curDead ? Integer.MAX_VALUE : cur.score();
            if (curScore <= best.score() + SWITCH_MARGIN && curScore != Integer.MAX_VALUE) return;
        }

        boolean firstSelection = cur == null;
        preferred = best;
        if (!firstSelection) lastSwitchMs = now;

        if (firstSelection) {
            Trace.debug("[P2P] conn={} initial path selection {} (rtt={}ms)", connId, best.label(), best.rtt());
        } else if (curDead) {
            Trace.warn("[P2P] conn={} preferred {} unusable, falling back to {} (rtt={}ms)",
                    connId, cur.label(), best.label(), best.rtt());
        } else {
            Trace.info("[P2P] conn={} switching {} -> {} (rtt={}ms){}",
                    connId, cur.label(), best.label(), best.rtt(),
                    relayFirst ? " [relay policy]" : "");
        }
        sender.onPathChanged();
        sender.releaseParked();
        hook.onSwitch(best, firstSelection);
    }

    public void send(byte[] data) throws IOException { send(data, 0, data.length); }

    public void send(byte[] data, int off, int len) throws IOException { sender.send(data, off, len); }

    public void drain(long timeoutMs) {
        long deadline = System.currentTimeMillis() + Math.max(0, timeoutMs);
        while (!closed) {
            if (sender.drained() && receiver.drained()) return;
            if (System.currentTimeMillis() >= deadline) {
                Trace.warn("[P2P] Drain timed out: {} fragments still unacknowledged", sender.unacked());
                return;
            }
            try { Thread.sleep(15); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public String routeLabel() {
        Path p = preferred;
        return p == null ? "not connected" : p.label();
    }

    public int routeRtt() {
        Path p = preferred;
        return p == null ? -1 : displayRtt(p);
    }

    public boolean routeDirect() {
        Path p = preferred;
        return p != null && p.via() == Path.Via.DIRECT;
    }

    public int directRtt() {
        for (Path p : paths.values()) {
            if (p.via() == Path.Via.DIRECT && p.usable()) return displayRtt(p);
        }
        return -1;
    }

    public boolean stalled() {
        return System.currentTimeMillis() - lastMoveMs > STALL_MS;
    }

    public boolean peerClosed() { return peerClosed; }

    public String stats() {
        StringBuilder sb = new StringBuilder();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Path> e : paths.entrySet()) {
            Path p = e.getValue();
            sb.append(String.format("[%s %s lastPacket=%s data=%s rtt=%d] ",
                    p.label(), p.state(),
                    p.lastInMs() == 0 ? "never" : (now - p.lastInMs()) + "msAgo",
                    p.lastAppDataMs() == 0 ? "never" : (now - p.lastAppDataMs()) + "msAgo",
                    p.rtt()));
        }
        return String.format("route=%s | %s | %s | unacked=%d", routeLabel(), sb,
                receiver.stats(), sender.unacked());
    }

    public void close() {
        if (closed) return;
        closed = true;
        Scheduler.unregister(beat);
        Path p = preferred;
        if (p != null) {

            for (int i = 0; i < 3; i++) {
                try {
                    p.send(Pack.build(connId, Pack.KIND_CLOSE));
                } catch (IOException ignored) {
                }
                if (i < 2) {
                    try { Thread.sleep(15); } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        for (Path q : paths.values()) q.markDead();
        paths.clear();
        preferred = null;
        sender.close();
        receiver.close();
        endpoint.unbind(connId);
    }
}
