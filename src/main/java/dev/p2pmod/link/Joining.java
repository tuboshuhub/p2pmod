package dev.p2pmod.link;

import dev.p2pmod.core.Connection;
import dev.p2pmod.core.Bytes;
import dev.p2pmod.core.Endpoint;
import dev.p2pmod.core.HostPort;
import dev.p2pmod.core.RetryableIOException;
import dev.p2pmod.core.Signaling;
import dev.p2pmod.core.RoomId;
import dev.p2pmod.core.SrvResolver;
import dev.p2pmod.core.StunProbe;
import dev.p2pmod.edge.Frontend;
import dev.p2pmod.support.Config;
import dev.p2pmod.support.RouteMode;
import dev.p2pmod.support.Trace;
import dev.p2pmod.support.Workers;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Joining implements Endpoint.RelayObserver {

    private static final long REGISTER_TIMEOUT_MS = 60_000;

    private static final long STALL_GRACE_MS = 12_000;

    private final String room;
    private final String relayHost;
    private final String rawTarget;

    private final boolean probeOnly;
    private volatile int relayPort;

    private Endpoint endpoint;
    private Frontend frontend;
    private volatile Connection connection;

    private volatile InetSocketAddress relayAddr;
    private volatile int myToken = -1;
    private volatile int peerToken = -1;
    private volatile InetSocketAddress peerAddr;

    private final boolean needSrv;
    private volatile CountDownLatch srvLatch;

    private final AtomicBoolean registered = new AtomicBoolean();
    private final CountDownLatch ready = new CountDownLatch(1);
    private volatile String failure;
    private volatile boolean closed;

    private volatile int gen;
    private volatile long stallSinceMs;

    public Joining(String target) {
        this(target, false);
    }

    public Joining(String target, boolean probeOnly) {
        this.rawTarget = target == null ? "" : target.trim();
        this.probeOnly = probeOnly;

        String roomName = RoomId.normalize(Config.defaultRoom);
        String hp = this.rawTarget;

        if (hp.regionMatches(true, 0, "p2p:", 0, 4)) hp = hp.substring(4).trim();
        int at = hp.indexOf('@');
        if (at > 0 && at < hp.length() - 1) {
            String r = hp.substring(0, at).trim();
            if (!r.isEmpty()) roomName = r;
            hp = hp.substring(at + 1);
        }

        HostPort.Split split = HostPort.split(hp, Signaling.DEFAULT_PORT);
        this.room = roomName;
        this.relayHost = split.host();
        this.relayPort = split.port();

        this.needSrv = !split.explicit();
    }

    public String room() { return room; }

    public String target() { return rawTarget; }

    public int localPort() { return frontend == null ? -1 : frontend.localPort(); }

    public int relayPort() { return relayPort; }

    public int start() throws IOException {
        return start(0);
    }

    public int start(int preferredPort) throws IOException {
        frontend = new Frontend(new Frontend.Owner() {
            @Override
            public void upstream(byte[] data, int off, int len) throws IOException {
                Connection c = connection;

                if (c == null) throw new RetryableIOException("tunnel not ready",
                        RetryableIOException.NOT_READY_BUDGET_MS);
                c.send(data, off, len);
            }

            @Override
            public void onDisconnected(String reason) {
                if (probeOnly) {

                    Connection c = connection;
                    connection = null;
                    if (c != null) {
                        try { c.close(); } catch (Throwable ignored) {}
                    }
                    Trace.debug("[P2P] Ping session idle ({}), keeping port {}", reason, localPort());
                    return;
                }
                Trace.info("[P2P] Local session ended ({}), releasing the tunnel", reason);
                close();
            }

            @Override
            public void onAccepted() {

                if (probeOnly) resetTunnel();
            }
        });
        int port = frontend.start(preferredPort);

        if (needSrv) {

            srvLatch = new CountDownLatch(1);
            Workers.run(() -> {
                try {
                    SrvResolver.Resolved r = SrvResolver.resolve(relayHost);
                    if (r != null && !closed) {
                        relayPort = r.port();
                        relayAddr = new InetSocketAddress(InetAddress.getByName(r.host()), r.port());
                        Trace.info("[P2P] SRV hit, relay address changed to {}:{}", r.host(), r.port());
                    }
                } catch (Throwable ignored) {
                } finally {
                    srvLatch.countDown();
                }
            });
        }

        openTunnel();

        Trace.info("[P2P] Local proxy listening on 127.0.0.1:{} (room={} relay={}:{})",
                port, room, relayHost, relayPort);
        return port;
    }

    private synchronized void openTunnel() throws IOException {
        int g = ++gen;

        Endpoint ep = probeOnly ? new Endpoint(0, Endpoint.PROBE_BUF_BYTES) : new Endpoint(0);
        ep.setRelayObserver(this);
        endpoint = ep;
        ep.start();

        Thread reg = new Thread(() -> registerLoop(g), "p2p-join-register-" + g);
        reg.setDaemon(true);
        reg.start();

        Thread keep = new Thread(() -> keepaliveLoop(g), "p2p-join-keepalive-" + g);
        keep.setDaemon(true);
        keep.start();
    }

    private synchronized void resetTunnel() {
        if (closed) return;
        Connection old = connection;
        connection = null;
        if (old != null) {

            try { old.close(); } catch (Throwable ignored) {}
        }
        Endpoint e = endpoint;
        endpoint = null;
        if (e != null) {
            try { e.close(); } catch (Throwable ignored) {}
        }
        myToken = -1;
        peerToken = -1;
        peerAddr = null;
        registered.set(false);
        stallSinceMs = 0;
        try {
            openTunnel();
        } catch (Throwable t) {
            Trace.warn("[P2P] Rebuilding the ping session failed: {}", t.toString());
        }
    }

    private void registerLoop(int g) {

        CountDownLatch latch = srvLatch;
        if (latch != null) {
            try {
                latch.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        long start = System.currentTimeMillis();
        long sentMs = 0;
        while (!closed && g == gen && !registered.get()) {
            long now = System.currentTimeMillis();
            if (now - start > REGISTER_TIMEOUT_MS) {
                fail("relay " + relayHost + ":" + relayPort + " did not respond within 60 seconds"
                        + " (check: UDP allowed through the tunnel/security group, the relay process alive, and the room name matching the server)");
                return;
            }
            if (failure != null) {
                fail("the relay rejected registration: " + failure);
                return;
            }
            if (peerAddr != null) {
                try {
                    ready.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                return;
            }
            long interval = (now - start < 1_500) ? 200 : 1_000;
            if (now - sentMs >= interval) {
                sentMs = now;
                sendRelay(Signaling.OP_REGISTER_CLIENT, 0, Signaling.registerBody(room, 0));
            }
            sleep(50);
        }
    }

    private void keepaliveLoop(int g) {
        while (!closed && g == gen) {
            if (myToken >= 0) {
                sendRelay(Signaling.OP_KEEPALIVE, myToken, new byte[0]);
            }
            watchStall();
            sleep(Config.keepaliveIntervalMs());
        }
    }

    private void watchStall() {
        Connection c = connection;
        if (c == null || closed) {
            stallSinceMs = 0;
            return;
        }
        if (!c.stalled()) {
            stallSinceMs = 0;
            return;
        }
        long now = System.currentTimeMillis();
        if (stallSinceMs == 0) {
            stallSinceMs = now;
        } else if (now - stallSinceMs > STALL_GRACE_MS) {

            boolean bothGone = c.linkDead(12_000);
            Trace.warn("[P2P] Tunnel stalled for {}ms ({}), disconnecting and prompting a reconnect",
                    now - stallSinceMs,
                    bothGone ? "relay and direct both silent" : "no ACKs, no delivery progress");
            stallSinceMs = 0;

            frontend.kickToMc("[P2P] " + net.minecraft.network.chat.Component
                    .translatable(bothGone ? "p2pmod.kick.both_down" : "p2pmod.kick.stalled")
                    .getString());
            close();
        }
    }

    public void setMode(RouteMode m) {
        Connection c = connection;
        if (c != null) c.setPreferRelay(m.preferRelay());
    }

    @Override
    public void onRelay(int op, int fromToken, int toToken, byte[] body, InetSocketAddress from) {
        if (closed) return;
        try {
            switch (op) {
                case Signaling.OP_REGISTER_OK -> {
                    if (body.length >= 4) {
                        myToken = Signaling.intBodyValue(body);
                        relayCanForward = Signaling.canForwardOf(body);
                        Trace.info("[P2P] Registered: token={} room={}", myToken, room);
                        if (!relayCanForward) {
                            Trace.info("[P2P] Relay {} is punch-only: it introduces peers but will not carry data",
                                    relayAddr);
                        }
                        maybeEstablish();
                    }
                }
                case Signaling.OP_PEER_INFO -> {
                    Signaling.PeerInfo info = Signaling.parsePeerInfo(body, body.length);
                    peerToken = info.token();
                    peerAddr = info.addr();
                    Trace.info("[P2P] Got the server endpoint {} (token={})", peerAddr, peerToken);
                    maybeEstablish();
                }
                case Signaling.OP_ERROR -> {
                    failure = Signaling.textBodyValue(body);

                    if (registered.get()) {
                        Trace.warn("[P2P] Room no longer available: {}", failure);
                        frontend.kickToMc("[P2P] " + failure);
                        close();
                    }
                }
                case Signaling.OP_EXIT -> {

                    Signaling.PeerInfo info = Signaling.parsePeerInfo(body, body.length);
                    if (info.addr() != null) {
                        exitHint = info.addr();
                        Trace.info("[P2P] Server exit pointed out by the relay: {}", exitHint);
                        Connection c = connection;
                        if (c != null) c.probeDirect(exitHint);
                    }
                }
                case Signaling.OP_RELAY -> {

                }
                default -> { }
            }
        } catch (Throwable t) {
            Trace.debug("Signalling handling failed: {}", t.toString());
        }
    }

    private volatile boolean relayCanForward = true;

    private volatile InetSocketAddress exitHint;

    private synchronized void maybeEstablish() {
        if (closed || registered.get() || myToken < 0 || peerAddr == null) return;
        registered.set(true);
        ready.countDown();

        Connection c = new Connection(myToken, endpoint, frontend::deliver);
        connection = c;

        c.setAllowDirect(!probeOnly && !Config.forceRelay);
        if (!relayCanForward && !probeOnly) {
            if (Config.forceRelay) {

                fail("this relay only helps with hole punching, so forceRelay cannot work here");
                return;
            }

            Trace.info("[P2P] Punch-only relay, relying on the direct path alone");
        } else {
            c.addRelay(relayAddr, myToken, peerToken);
        }
        c.setSwitchHook((p, initial) -> {
            Trace.info("[P2P] Link switched -> {} ({}ms)", p.label(), p.rtt());

            if (!initial) Portal.notifySwitch(p.label(), p.rtt());
        });

        if (!probeOnly) c.setPreferRelay(Config.mode.preferRelay());

        if (!probeOnly && !Config.forceRelay && exitHint != null) {
            c.probeDirect(exitHint);
        }
        if (!probeOnly && !Config.forceRelay) {
            InetSocketAddress punch = punchTarget(peerAddr);
            if (punch != null && c.probeDirect(punch) != null && !punch.equals(peerAddr)) {
                Trace.info("[P2P] Server shares the relay host (the relay sees it as {}), punching {} directly",
                        peerAddr, punch);
            }
        }

        if (!probeOnly && !Config.forceRelay) {
            InetSocketAddress relay = relayAddr;
            c.announceLocalV6(relay == null || Bytes.isPrivate(relay.getAddress()));
        }
        if (!probeOnly && !Config.forceRelay && Config.stunEnabled) {
            Workers.run(() -> {
                StunProbe.Result r = StunProbe.probe(endpoint, Config.stunServers);
                if (r != null && !closed) c.announceExit(r.mapped());
            });
        }
        Trace.info("[P2P] Tunnel ready: connId={} room={}", myToken, room);
    }

    private InetSocketAddress punchTarget(InetSocketAddress fromRelay) {
        if (fromRelay == null) return null;
        InetAddress ip = fromRelay.getAddress();
        if (ip != null && !ip.isLoopbackAddress() && !ip.isAnyLocalAddress()) return fromRelay;
        InetSocketAddress relay = relayAddr;
        InetAddress relayIp = relay == null ? null : relay.getAddress();
        if (relayIp == null || relayIp.isLoopbackAddress() || relayIp.isAnyLocalAddress()) return null;
        return new InetSocketAddress(relayIp, fromRelay.getPort());
    }

    private void fail(String msg) {
        if (closed) return;
        Trace.error("[P2P] {}", msg);
        frontend.kickToMc("[P2P] " + msg);
        close();
    }

    public Connection connection() { return connection; }

    public int myToken() { return myToken; }

    public boolean ready() { return registered.get(); }

    public boolean closed() { return closed; }

    public void close() {
        if (closed) return;
        closed = true;
        if (myToken >= 0 && endpoint != null) {
            sendRelay(Signaling.OP_UNREGISTER_CLIENT, myToken, new byte[0]);
        }
        Connection c = connection;
        if (c != null) c.close();
        if (frontend != null) frontend.close();
        if (endpoint != null) endpoint.close();
    }

    private void sendRelay(int op, int token, byte[] body) {
        try {
            InetSocketAddress a = relayAddr;

            if (a == null) {
                a = new InetSocketAddress(InetAddress.getByName(relayHost), relayPort);
                relayAddr = a;
            }
            endpoint.send(a, Signaling.control(op, token, body));
        } catch (IOException e) {
            Trace.debug("Relay signalling send failed: {}", e.toString());
        }
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
