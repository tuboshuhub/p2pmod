package dev.p2pmod.link;

import dev.p2pmod.core.Connection;
import dev.p2pmod.core.Endpoint;
import dev.p2pmod.core.RoomId;
import dev.p2pmod.core.Signaling;
import dev.p2pmod.core.StunProbe;
import dev.p2pmod.edge.Backend;
import dev.p2pmod.support.Config;
import dev.p2pmod.support.Scheduler;
import dev.p2pmod.support.Trace;
import dev.p2pmod.support.Workers;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class Hosting implements Endpoint.RelayObserver {

    private static final long IDLE_REAP_MS = 300_000;
    private static final long NEVER_USED_MS = 60_000;

    private static final long CLIENT_STALL_REAP_MS = 20_000;

    private volatile String room;
    private final String relayHost;
    private final int relayPort;
    private final int mcPort;

    private Endpoint endpoint;
    private volatile InetSocketAddress relayAddr;
    private volatile int hostToken = -1;
    private volatile boolean running;

    private final Map<Integer, Client> clients = new ConcurrentHashMap<>();
    private volatile CountDownLatch registerLatch;
    private volatile StunProbe.Result stun;

    private volatile boolean relayCanForward = true;
    private final Object stunLock = new Object();

    public String publicIp() {
        dev.p2pmod.core.StunProbe.Result r = stun;
        java.net.InetAddress a = r == null || r.mapped() == null ? null : r.mapped().getAddress();

        if (a != null && (a.isLoopbackAddress() || a.isSiteLocalAddress() || a.isLinkLocalAddress()
                || a.isAnyLocalAddress() || a.isMulticastAddress())) {
            a = null;
        }
        if (a != null) return a.getHostAddress();

        java.util.List<java.net.InetAddress> v6 = dev.p2pmod.core.Connection.localV6Addresses(false);
        return v6.isEmpty() ? null : v6.get(0).getHostAddress();
    }

    private StunProbe.Result stunResult() {

        if (!Config.stunEnabled) return null;
        StunProbe.Result r = stun;
        if (r != null) return r;
        synchronized (stunLock) {
            if (stun == null) stun = StunProbe.probe(endpoint, Config.stunServers);
            return stun;
        }
    }
    private volatile String registerError;

    private final Thread keepThread;
    private final Runnable beat = this::beatSafely;

    public static final class Client {
        final int token;
        final InetSocketAddress addr;
        final Connection conn;
        final Backend backend;
        volatile long createdMs = System.currentTimeMillis();
        volatile long closedMs;
        volatile long stalledSince;
        volatile boolean everUp;

        Client(int token, InetSocketAddress addr, Connection conn, Backend backend) {
            this.token = token;
            this.addr = addr;
            this.conn = conn;
            this.backend = backend;
        }
    }

    public Hosting(String relayHost, int relayPort, String room, int mcPort) {
        this.relayHost = relayHost;
        this.relayPort = relayPort;
        String wanted = (room == null || room.isBlank()) ? Config.defaultRoom : room.trim();
        this.room = RoomId.normalize(wanted);
        if (!RoomId.acceptable(this.room)) {
            Trace.warn("[P2P] Room id {} is not valid (expected 4 letters or digits, case sensitive); "
                    + "the relay may reject it", this.room);
        }
        this.mcPort = mcPort;
        this.keepThread = new Thread(this::keepaliveLoop, "p2p-host-keepalive");
    }

    public String room() { return room; }

    public int mcPort() { return mcPort; }

    public String relayHost() { return relayHost; }

    public int relayPort() { return relayPort; }

    public Endpoint endpoint() { return endpoint; }

    public void start() throws IOException, InterruptedException {
        relayAddr = new InetSocketAddress(InetAddress.getByName(relayHost), relayPort);

        int want = Config.hostPunchPort;
        try {
            endpoint = new Endpoint(want);
        } catch (SocketException e) {
            Trace.warn("[P2P] Hole-punch port {} could not be bound ({}), using a random port", want, e.toString());
            endpoint = new Endpoint(0);
        }
        if (want > 0 && endpoint.localPort() != want) {
            Trace.warn("[P2P] Requested port {} did not take effect, actually using {} -- the security group / DNAT "
                    + "must allow UDP {}, otherwise clients cannot punch through and links stay on the relay",
                    want, endpoint.localPort(), endpoint.localPort());
        }
        endpoint.setRelayObserver(this);
        endpoint.start();
        running = true;

        Workers.run(() -> {
            try {
                stunResult();
            } catch (Throwable ignored) {
            }
        });

        boolean ok = attemptRegister(room);
        if (!ok) {

            Trace.warn("[P2P] Registering room {} failed ({}) -- room id and relay address reset to defaults and saved",
                    room, registerError == null ? "no response from the relay" : registerError);
            room = RoomId.DEFAULT;
            Config.saveRoom("");
            Config.saveRelay("", Signaling.DEFAULT_PORT);
            close();
            throw new IOException("Could not register with relay " + relayHost + ":" + relayPort
                    + " for room " + room + ": " + (registerError == null ? "no response" : registerError));        }

        Config.saveRoom(RoomId.DEFAULT.equals(room) ? "" : room);
        Config.saveRelay(relayHost, relayPort);

        keepThread.setDaemon(true);
        keepThread.start();
        Scheduler.register(beat);

        Trace.info("[P2P] Registered: relay={}:{} room={} localMC={} punchUDP={}",
                relayHost, relayPort, room, mcPort, endpoint.localPort());
    }

    private boolean attemptRegister(String roomName) throws InterruptedException {
        registerError = null;
        hostToken = -1;
        registerLatch = new CountDownLatch(1);
        sendRelay(Signaling.OP_REGISTER_HOST, 0, Signaling.registerBody(roomName, 0));
        if (!registerLatch.await(8, TimeUnit.SECONDS)) return false;
        return registerError == null && hostToken >= 0;
    }

    private void keepaliveLoop() {
        while (running) {
            if (hostToken >= 0) sendRelay(Signaling.OP_KEEPALIVE, hostToken, new byte[0]);
            sleep(Config.keepaliveIntervalMs());
        }
    }

    @Override
    public void onRelay(int op, int fromToken, int toToken, byte[] body, InetSocketAddress from) {
        if (!running) return;
        try {
            switch (op) {
                case Signaling.OP_REGISTER_OK -> {
                    if (body.length >= 4) {
                        hostToken = Signaling.tokenOfRegisterOk(body);
                        relayCanForward = Signaling.canForwardOfRegisterOk(body);
                        if (!relayCanForward) {
                            Trace.info("[P2P] Relay {} is punch-only: clients must reach this host directly", relayHost);
                        }
                        String actual = Signaling.roomOfRegisterOk(body);
                        if (actual != null && !actual.isBlank() && !actual.equals(room)) {

                            room = actual;
                            Trace.warn("[P2P] Room was taken, the relay reassigned {} -- tell clients this room id",
                                    actual);
                        }
                        CountDownLatch l = registerLatch;
                        if (l != null) l.countDown();
                    }
                }
                case Signaling.OP_ERROR -> {
                    registerError = Signaling.textBodyValue(body);
                    CountDownLatch l = registerLatch;
                    if (l != null) l.countDown();
                }
                case Signaling.OP_PEER_INFO -> {
                    Signaling.PeerInfo info = Signaling.parsePeerInfo(body, body.length);
                    admit(info.token(), info.addr());
                }
                default -> { }
            }
        } catch (Throwable t) {
            Trace.debug("Server signalling failed: {}", t.toString());
        }
    }

    private void admit(int clientToken, InetSocketAddress clientAddr) {
        if (clientToken <= 0 || clients.containsKey(clientToken)) return;
        if (!Config.acceptBeforeReady) {
            Trace.debug("[P2P] Not accepting yet, ignoring client {}", clientToken);
            return;
        }

        Backend backend = new Backend(new Backend.Owner() {
            @Override
            public void upstream(byte[] data, int off, int len) throws IOException {
                Client c = clients.get(clientToken);
                if (c == null || c.conn == null) throw new IOException("connection already reclaimed");
                c.conn.send(data, off, len);
            }

            @Override
            public void onClosed() {
                Client c = clients.get(clientToken);
                if (c != null) {
                    c.closedMs = System.currentTimeMillis();
                }
            }
        });

        Connection conn = new Connection(clientToken, endpoint, data -> {
            Client c = clients.get(clientToken);
            if (c == null) return;
            ensureMc(c);
            if (c.backend.connected()) c.backend.deliver(data);
        });

        if (relayCanForward) {
            conn.addRelay(relayAddr, hostToken, clientToken);
        } else {
            Trace.info("[P2P] Punch-only relay: client {} has no relay leg, direct only", clientToken);
        }
        conn.setSwitchHook((p, initial) -> Trace.debug("[P2P] Client {} link switched -> {} ({}ms)",
                clientToken, p.label(), p.rtt()));
        if (clientAddr != null && !Config.forceRelay) conn.probeDirect(clientAddr);

        if (!Config.forceRelay) {
            java.net.InetSocketAddress relay = relayAddr;
            conn.announceLocalV6(relay == null || dev.p2pmod.core.Bytes.isPrivate(relay.getAddress()));
        }

        if (!Config.forceRelay && Config.stunEnabled) {
            Workers.run(() -> {
                StunProbe.Result r = stunResult();
                if (r == null) return;
                if (conn != null) conn.announceExit(r.mapped());

                sendExitHint(clientToken, r.mapped());
            });
        } else if (stun != null) {

            sendExitHint(clientToken, stun.mapped());
        }

        Client c = new Client(clientToken, clientAddr, conn, backend);
        clients.put(clientToken, c);
        Trace.debug("[P2P] Client {} admitted (public addr={})", clientToken, clientAddr);
    }

    private void sendExitHint(int clientToken, java.net.InetSocketAddress addr) {
        if (addr == null || hostToken < 0) return;
        sendRelay(Signaling.OP_EXIT, hostToken, Signaling.peerInfoBody(clientToken, addr));
    }

    private synchronized void ensureMc(Client c) {
        if (c.backend.connected()) return;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                c.backend.connect("127.0.0.1", mcPort);
                c.everUp = true;
                c.closedMs = 0;
                Trace.debug("[P2P] Client {} attached to local MC :{}", c.token, mcPort);
                return;
            } catch (IOException e) {
                if (attempt == 2) {
                    Trace.warn("[P2P] Client {} failed to reach local MC :{} ({}) -- dropping this client",
                            c.token, mcPort, e.toString());
                    evict(c.token);
                    return;
                }
                sleep(250);
            }
        }
    }

    private void beatSafely() {
        try { beat(); } catch (Throwable t) { Trace.debug("Server heartbeat failed: {}", t.toString()); }
    }

    private void beat() {
        if (!running) return;
        long now = System.currentTimeMillis();
        for (Client c : clients.values()) {
            if (c.conn != null && c.conn.peerClosed()) {

                Trace.info("[P2P] Client {} left cleanly, reclaiming tunnel", c.token);
                evict(c.token);
            } else if (c.closedMs != 0 && now - c.closedMs > IDLE_REAP_MS) {
                Trace.info("[P2P] Client {} idle too long, reclaiming", c.token);
                evict(c.token);
            } else if (!c.everUp && now - c.createdMs > NEVER_USED_MS) {
                Trace.info("[P2P] Client {} registered but never sent data, reclaiming", c.token);
                evict(c.token);
            } else if (c.conn != null && c.conn.stalled()) {

                if (c.stalledSince == 0) {
                    c.stalledSince = now;
                } else if (now - c.stalledSince > CLIENT_STALL_REAP_MS) {
                    Trace.warn("[P2P] Client {} link stalled for {}ms, reclaiming this tunnel (stops futile retransmission)",
                            c.token, now - c.stalledSince);
                    evict(c.token);
                }
            } else {
                c.stalledSince = 0;
            }
        }
    }

    private void evict(int token) {
        Client removed = clients.remove(token);
        if (removed != null) {
            removed.conn.close();
            removed.backend.close();
        }
    }

    public int clientCount() { return clients.size(); }

    public int directClients() {
        int n = 0;
        for (Client c : clients.values()) {
            if (c.conn.routeDirect()) n++;
        }
        return n;
    }

    public record Route(boolean direct, int rtt) {}

    public Map<Integer, Route> routesByLocalPort() {
        Map<Integer, Route> out = new java.util.HashMap<>();
        for (Client c : clients.values()) {
            int port = c.backend.localPort();
            if (port <= 0) continue;
            out.put(port, new Route(c.conn.routeDirect(), c.conn.routeRtt()));
        }
        return out;
    }

    public void inspect() {
        for (Client c : clients.values()) {
            Trace.info("[P2P] Client {} route={} rtt={}ms | {}",
                    c.token, c.conn.routeLabel(), c.conn.routeRtt(), c.conn.stats());
        }
    }

    public void close() {
        if (!running) return;
        running = false;

        if (hostToken >= 0 && endpoint != null && relayAddr != null) {
            for (int i = 0; i < 3; i++) {
                try {
                    endpoint.send(relayAddr,
                            Signaling.control(Signaling.OP_UNREGISTER_HOST, hostToken, new byte[0]));
                } catch (IOException ignored) {
                }
                if (i < 2) sleep(50);
            }
        }
        for (Integer token : clients.keySet()) evict(token);
        if (endpoint != null) endpoint.close();
        Scheduler.unregister(beat);
    }

    private void sendRelay(int op, int token, byte[] body) {
        try {
            endpoint.send(relayAddr, Signaling.control(op, token, body));
        } catch (IOException e) {
            Trace.debug("Server signalling send failed: {}", e.toString());
        }
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
