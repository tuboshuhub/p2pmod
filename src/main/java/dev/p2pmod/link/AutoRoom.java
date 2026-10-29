package dev.p2pmod.link;

import dev.p2pmod.relay.RelayService;
import dev.p2pmod.core.RoomId;
import dev.p2pmod.support.Config;
import dev.p2pmod.support.Scheduler;
import dev.p2pmod.support.Trace;

public final class AutoRoom {

    private AutoRoom() {}

    private static RelayService relay;

    private static volatile String lanRoom;
    private static volatile int lanPort = -1;
    private static final Runnable heartbeat = AutoRoom::beat;

    public static void bootstrapRelay() {
        if (relay != null) return;
        if (!Config.relayEmbedded) {
            Trace.info("[P2P] Embedded relay disabled (relay.embedded=false); this machine will act as a server using external relay {}:{}",
                    Config.relayHost.isBlank() ? "<unset, must be given via /p2p>" : Config.relayHost,
                    Config.relayPort);
            return;
        }
        int port = Config.relayPort;
        try {
            relay = new RelayService();
            relay.start(port);
        } catch (Exception e) {
            Trace.warn("[P2P] Embedded relay failed to start (port {} may be taken): {}", port, e.toString());
            relay = null;
        }
    }

    public static void shutdownRelay() {
        if (relay != null) {
            relay.stop();
            relay = null;
        }
        Scheduler.unregister(heartbeat);
    }

    public static void onServerReady(int mcPort) {

        try {
            String room = openRoom(RoomId.normalize(Config.defaultRoom), mcPort);
            Trace.info("[P2P] Auto-registered: room={} MC port={}", room, mcPort);
        } catch (Exception e) {
            Trace.warn("[P2P] Auto-registration failed: {}", e.toString());
        }
    }

    public static synchronized String onLanOpened(int mcPort) throws Exception {
        if (lanRoom != null) {
            Hosting current = Portal.room();
            if (current == null || !lanRoom.equals(current.room())) {

                lanRoom = null;
                lanPort = -1;
                return null;
            }

            if (lanPort == mcPort) return null;
        }
        String room = lanRoom != null ? lanRoom : RoomId.random();
        String actual = openRoom(room, mcPort);
        lanRoom = actual;
        lanPort = mcPort;
        Trace.info("[P2P] LAN world opened: room={} MC port={}", actual, mcPort);
        return actual;
    }

    private static synchronized String openRoom(String roomName, int mcPort) throws Exception {

        boolean useEmbedded = Config.relayHost.isBlank()
                || (Config.relayEmbedded && Config.isLoopback(Config.relayHost));
        bootstrapRelay();
        if (useEmbedded && relay == null) {

            throw new IllegalStateException("embedded relay is not running (port " + Config.relayPort
                    + " already in use?)");
        }

        String host = useEmbedded ? "127.0.0.1" : Config.relayHost;
        try {
            Portal.host(host, Config.relayPort, roomName, mcPort);
        } finally {
            Scheduler.register(heartbeat);
        }
        Hosting h = Portal.room();
        return h == null ? roomName : h.room();
    }

    public static void onServerStopped() {
        lanRoom = null;
        lanPort = -1;
        Portal.stopRoom();
        shutdownRelay();
    }

    private static volatile long lastBeatMs;

    private static void beat() {
        long now = System.currentTimeMillis();
        if (now - lastBeatMs < 5_000) return;
        lastBeatMs = now;
        if (relay != null) {
            Trace.debug("[P2P][relay] {}", relay.stats());
        }
    }

    public static RelayService relay() { return relay; }
}
