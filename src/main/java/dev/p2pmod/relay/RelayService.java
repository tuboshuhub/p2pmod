package dev.p2pmod.relay;

import dev.p2pmod.core.Endpoint;
import dev.p2pmod.core.RoomId;
import dev.p2pmod.core.Signaling;
import dev.p2pmod.support.Scheduler;
import dev.p2pmod.support.Trace;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class RelayService {

    private static final long CLIENT_IDLE_MS = 25_000;

    private static final class Peer {
        final int token;
        final String room;
        final boolean host;
        volatile InetSocketAddress addr;
        volatile long lastSeen = System.currentTimeMillis();
        final long firstSeen = System.currentTimeMillis();

        Peer(int token, String room, boolean host, InetSocketAddress addr) {
            this.token = token;
            this.room = room;
            this.host = host;
            this.addr = addr;
        }
    }

    private static final int REG_BURST = 20;
    private static final long REG_REFILL_MS = 60_000;
    private static final class RegBucket {
        double tokens = REG_BURST;
        long lastMs = System.currentTimeMillis();
    }
    private final Map<String, RegBucket> regLimits = new ConcurrentHashMap<>();

    private final java.security.SecureRandom tokenSource = new java.security.SecureRandom();
    private final Map<Integer, Peer> peers = new ConcurrentHashMap<>();
    private final Map<String, Integer> hostByRoom = new ConcurrentHashMap<>();
    private final Map<Integer, Set<Integer>> clientsOfHost = new ConcurrentHashMap<>();

    private Endpoint endpoint;
    private volatile boolean running;

    private final Runnable reaper = this::reapSafely;

    public void start(int port) throws IOException {
        endpoint = new Endpoint(port);
        endpoint.setRelayObserver(this::onRelay);
        endpoint.start();
        running = true;
        Scheduler.register(reaper);
        Trace.info("[P2P][relay] Listening on UDP :{}", port);
    }

    public int port() {
        return endpoint == null ? -1 : endpoint.localPort();
    }

    public void stop() {
        if (!running) return;
        running = false;
        Scheduler.unregister(reaper);
        peers.clear();
        hostByRoom.clear();
        clientsOfHost.clear();
        if (endpoint != null) endpoint.close();
    }

    private void onRelay(int op, int fromToken, int toToken, byte[] body, InetSocketAddress from) {
        if (!running) return;
        try {
            switch (op) {
                case Signaling.OP_REGISTER_HOST   -> registerHost(body, from);
                case Signaling.OP_REGISTER_CLIENT -> registerClient(body, from);
                case Signaling.OP_RELAY           -> forward(fromToken, toToken, body, from);
                case Signaling.OP_KEEPALIVE       -> touch(fromToken, from);
                case Signaling.OP_UNREGISTER_HOST -> unregisterHost(fromToken);
                case Signaling.OP_UNREGISTER_CLIENT -> unregisterClient(fromToken);
                case Signaling.OP_PEER_REQ        -> resendPeerInfo(fromToken, body);
                case Signaling.OP_EXIT            -> relayExit(fromToken, toToken, body, from);
                default -> { }
            }
        } catch (Throwable t) {
            Trace.debug("[P2P][relay] Handling failed: {}", t.toString());
        }
    }

    private boolean allowRegister(InetSocketAddress from) {
        if (from == null || from.getAddress() == null) return true;

        if (from.getAddress().isLoopbackAddress()) return true;
        String ip = from.getAddress().getHostAddress();
        RegBucket b = regLimits.computeIfAbsent(ip, k -> new RegBucket());
        long now = System.currentTimeMillis();
        synchronized (b) {
            long elapsed = now - b.lastMs;
            if (elapsed > 0) {
                b.tokens = Math.min(REG_BURST, b.tokens + (double) elapsed / REG_REFILL_MS);
                b.lastMs = now;
            }
            if (b.tokens < 1.0) {
                Trace.periodic("[P2P][relay] Registration rate limit hit by {}", ip);
                return false;
            }
            b.tokens -= 1.0;
            return true;
        }
    }

    private void registerHost(byte[] body, InetSocketAddress from) {
        if (!allowRegister(from)) return;
        String wanted = RoomId.normalize(Signaling.roomOf(body));
        String room = wanted;

        if (hostOnline(wanted)) {

            String alt = null;
            for (int i = 0; i < 64; i++) {
                String cand = RoomId.random();
                if (!hostOnline(cand)) { alt = cand; break; }
            }
            if (alt == null) {
                send(from, Signaling.OP_ERROR, 0,
                        Signaling.textBody("room " + wanted + " is taken and no new room could be assigned"));
                return;
            }
            room = alt;
            Trace.info("[P2P][relay] Room {} was taken, reassigned {}", wanted, room);
        }

        int t = newToken();
        Peer host = new Peer(t, room, true, from);
        peers.put(t, host);
        hostByRoom.put(room, t);
        clientsOfHost.putIfAbsent(t, ConcurrentHashMap.newKeySet());
        send(from, Signaling.OP_REGISTER_OK, 0, Signaling.registerOkBody(t, room, canForward()));
        Trace.info("[P2P][relay] Server registered: room={} token={} from={}", room, t, from);
        logState();
    }

    private int newToken() {
        for (int attempt = 0; attempt < 64; attempt++) {
            int t = tokenSource.nextInt(Integer.MAX_VALUE - 1) + 1;
            if (!peers.containsKey(t)) return t;
        }

        int t = 1;
        while (peers.containsKey(t)) t++;
        return t;
    }

    private void logState() {
        try {
            Trace.info("[P2P][relay] {}", stats());
        } catch (Throwable ignored) {
        }
    }

    private boolean hostOnline(String room) {
        Integer token = hostByRoom.get(room);
        if (token == null) return false;
        Peer h = peers.get(token);
        return h != null && h.host && System.currentTimeMillis() - h.lastSeen < idleTimeoutMs();
    }

    private void reapRegLimits(long now) {
        if (regLimits.size() < 512) return;
        regLimits.entrySet().removeIf(e -> {
            RegBucket b = e.getValue();
            synchronized (b) {
                return now - b.lastMs > REG_REFILL_MS * 10;
            }
        });
    }

    private static long idleTimeoutMs() {
        return dev.p2pmod.support.Config.relayIdleTimeoutSec * 1000L;
    }

    private void registerClient(byte[] body, InetSocketAddress from) {
        if (!allowRegister(from)) return;
        String room = Signaling.roomOf(body);
        Integer hostToken = hostByRoom.get(room);
        if (hostToken == null) {

            send(from, Signaling.OP_ERROR, 0, Signaling.textBody("room not available: " + room));
            return;
        }
        Peer host = peers.get(hostToken);
        if (host == null) {
            send(from, Signaling.OP_ERROR, 0, Signaling.textBody("room not available: " + room));
            return;
        }

        int t = newToken();
        Peer client = new Peer(t, room, false, from);
        peers.put(t, client);
        clientsOfHost.computeIfAbsent(hostToken, k -> ConcurrentHashMap.newKeySet()).add(t);

        send(from, Signaling.OP_REGISTER_OK, 0, Signaling.intBody(t, canForward()));

        send(from, Signaling.OP_PEER_INFO, 0, Signaling.peerInfoBody(hostToken, host.addr));
        send(host.addr, Signaling.OP_PEER_INFO, 0, Signaling.peerInfoBody(t, client.addr));

        host.lastSeen = System.currentTimeMillis();
        Trace.info("[P2P][relay] Client registered: room={} token={} from={}", room, t, from);
        logState();
    }

    private void relayExit(int fromToken, int toToken, byte[] body, InetSocketAddress from) {
        Peer sender = fromToken > 0 ? peers.get(fromToken) : null;

        Peer target = peers.get(toToken != 0 ? toToken : Signaling.intBodyValue(body));
        if (sender == null || target == null || !sender.room.equals(target.room)) return;
        if (!samePublicIp(sender.addr, from)) return;
        sender.addr = from;
        sender.lastSeen = System.currentTimeMillis();
        if (target.addr == null) return;
        try {
            endpoint.send(target.addr, Signaling.control(Signaling.OP_EXIT, sender.token, body));
        } catch (IOException ignored) {
        }
        Trace.debug("[P2P][relay] Exit hint {} -> {}", sender.token, target.token);
    }

    public boolean dualStack() {
        return endpoint != null && endpoint.isDualStack();
    }

    private boolean canForward() {
        return dev.p2pmod.support.Config.relayForward;
    }

    private void forward(int fromToken, int toToken, byte[] payload, InetSocketAddress from) {
        if (!canForward()) {

            Trace.periodic("[P2P][relay] Punch-only relay refused to forward {}B", payload.length);
            send(from, Signaling.OP_ERROR, 0,
                    Signaling.textBody("this relay only helps with hole punching; it does not forward game data"));
            return;
        }
        Peer target = peers.get(toToken);
        if (target == null) {
            Trace.debug("[P2P][relay] Target token={} does not exist, dropping {}B", toToken, payload.length);
            return;
        }

        Peer sender = fromToken > 0 ? peers.get(fromToken) : null;
        if (sender == null) {
            Trace.periodic("[P2P][relay] Dropped forward to token={}: sender token={} is not registered",
                    toToken, fromToken);
            return;
        }
        if (!sender.room.equals(target.room)) {
            Trace.periodic("[P2P][relay] Dropped forward {} -> {}: sender is in room {}, target in room {}",
                    sender.token, toToken, sender.room, target.room);
            return;
        }
        if (!samePublicIp(sender.addr, from)) {

            Trace.periodic("[P2P][relay] Dropped forward from token={}: source {} is not the registered egress {}",
                    sender.token, from, sender.addr);
            return;
        }
        sender.addr = from;
        sender.lastSeen = System.currentTimeMillis();
        if (target.addr == null) return;
        try {
            endpoint.send(target.addr, Signaling.forward(sender.token, toToken, payload));
        } catch (IOException ignored) {
        }
    }

    private static boolean samePublicIp(InetSocketAddress a, InetSocketAddress b) {
        if (a == null || b == null) return false;

        java.net.InetAddress x = dev.p2pmod.core.Bytes.normalize(a);
        java.net.InetAddress y = dev.p2pmod.core.Bytes.normalize(b);
        return x != null && x.equals(y);
    }

    private void touch(int token, InetSocketAddress from) {
        Peer p = peers.get(token);
        if (p == null) return;

        if (!p.addr.equals(from)) {
            if (!samePublicIp(p.addr, from)) {
                Trace.periodic("[P2P][relay] token={} keepalive from a different IP {} (registered {}), ignored: "
                        + "a token cannot be moved to another host, that peer has to re-register", token, from, p.addr);
                return;
            }
            Trace.info("[P2P][relay] token={} egress address changed {} -> {}", token, p.addr, from);
            p.addr = from;
        }
        p.lastSeen = System.currentTimeMillis();
    }

    private void unregisterHost(int token) {
        Peer p = peers.remove(token);
        if (p == null || !p.host) return;
        hostByRoom.remove(p.room, token);
        Set<Integer> cs = clientsOfHost.remove(token);
        if (cs != null) {
            for (int c : cs) {
                Peer cp = peers.get(c);

                if (cp != null) {
                    send(cp.addr, Signaling.OP_ERROR, 0,
                            Signaling.textBody("room " + p.room + " closed (server offline)"));
                }
                peers.remove(c);
            }
        }
        Trace.info("[P2P][relay] Server unregistered: room={} token={}", p.room, token);
        logState();
    }

    private void resendPeerInfo(int requesterToken, byte[] body) {
        int wanted = Signaling.intBodyValue(body);
        Peer requester = peers.get(requesterToken);
        Peer target = peers.get(wanted);
        if (requester == null || target == null) return;
        if (target.host) {
            send(requester.addr, Signaling.OP_PEER_INFO, 0,
                    Signaling.peerInfoBody(wanted, target.addr));
        }
    }

    private void reapSafely() {
        try { reap(); } catch (Throwable ignored) {}
    }

    private void unregisterClient(int token) {
        Peer p = peers.remove(token);
        if (p == null || p.host) return;
        detachClient(token);
        Trace.info("[P2P][relay] Client unregistered: room={} token={}", p.room, token);
        logState();
    }

    private void detachClient(int token) {
        for (Set<Integer> cs : clientsOfHost.values()) {
            cs.remove(token);
        }
    }

    private void reap() {
        long now = System.currentTimeMillis();
        reapRegLimits(now);
        List<Peer> deadHosts = new ArrayList<>();
        List<Peer> deadClients = new ArrayList<>();
        for (Peer p : peers.values()) {
            long limit = p.host ? idleTimeoutMs() : CLIENT_IDLE_MS;
            if (now - p.lastSeen <= limit) continue;
            if (p.host) deadHosts.add(p);
            else deadClients.add(p);
        }
        for (Peer p : deadHosts) {
            Trace.info("[P2P][relay] Server (token={}) silent for more than {}s -- room {} auto-unregistered (server offline)",
                    p.token, dev.p2pmod.support.Config.relayIdleTimeoutSec, p.room);
            unregisterHost(p.token);
        }
        for (Peer p : deadClients) {
            peers.remove(p.token);
            detachClient(p.token);
            Trace.info("[P2P][relay] Client timed out: room={} token={} ({} seconds without traffic)",
                    p.room, p.token, CLIENT_IDLE_MS / 1000);
        }
        if (!deadClients.isEmpty()) logState();
    }

    private void send(InetSocketAddress to, int op, int token, byte[] body) {
        try {
            endpoint.send(to, Signaling.control(op, token, body));
        } catch (IOException ignored) {
        }
    }

    public String stats() {
        if (hostByRoom.isEmpty()) {
            return peers.isEmpty() ? "no rooms" : "no rooms leftoverConnections=" + peers.size();
        }
        long now = System.currentTimeMillis();
        StringBuilder all = new StringBuilder();
        for (String room : hostByRoom.keySet()) {
            List<Peer> list = new ArrayList<>();
            for (Peer p : peers.values()) {
                if (room.equals(p.room)) list.add(p);
            }
            StringBuilder sb = new StringBuilder();
            for (Peer p : list) {
                sb.append(String.format("[%s#%d %dsAgo registered %s] ",
                        p.host ? "server" : "client", p.token,
                        (now - p.lastSeen) / 1000, duration(now - p.firstSeen)));
            }
            all.append(String.format("room=%s peers=%d %s", room, list.size(), sb));
        }
        return all.toString().trim();
    }

    private static String duration(long ms) {
        long s = ms / 1000;
        if (s < 60) return s + "s";
        long m = s / 60;
        if (m < 60) return m + "m" + (s % 60) + "s";
        return (m / 60) + "h" + (m % 60) + "m";
    }
}
