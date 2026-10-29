package dev.p2pmod.link;

import dev.p2pmod.net.RouteCache;
import dev.p2pmod.support.Config;
import dev.p2pmod.support.RouteMode;
import dev.p2pmod.support.Trace;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;

public final class Portal {

    private Portal() {}

    private static volatile Joining main;
    private static volatile Hosting room;

    private static final LinkedHashMap<String, Joining> PROBES =
            new LinkedHashMap<>(8, 0.75f, true);
    private static final int PROBE_MAX = 6;
    private static final Object PROBE_LOCK = new Object();

    private static final java.util.Map<String, Long> PROBE_SEEN = new java.util.concurrent.ConcurrentHashMap<>();
    private static final long PROBE_IDLE_MS = 30_000;

    static {

        dev.p2pmod.support.Scheduler.register(Portal::reapIdleProbes);
    }

    private static final int PROBE_PORT_BASE = 25560;
    private static final int PROBE_PORT_SLOTS = 40;

    public record Status(String route, int rtt, int directRtt, boolean active) {
        public static final Status IDLE = new Status("not connected", -1, -1, false);
    }

    public static synchronized int join(String target) throws IOException {
        leave();
        Joining j = new Joining(target);
        int port = j.start();
        main = j;
        return port;
    }

    public static synchronized void leave() {
        Joining j = main;
        main = null;
        if (j != null) j.close();

        RouteCache.clear();
    }

    public static int pingStatus(String target) throws IOException {
        String t = target == null ? "" : target.trim();
        int want = PROBE_PORT_BASE + Math.floorMod(t.hashCode(), PROBE_PORT_SLOTS);

        Joining j;
        Joining victim = null;
        synchronized (PROBE_LOCK) {
            PROBE_SEEN.put(t, System.currentTimeMillis());
            j = PROBES.get(t);
            if (j != null && j.closed()) {
                PROBES.remove(t);
                j = null;
            }
            if (j == null) {
                j = new Joining(t, true);
                PROBES.put(t, j);
                if (PROBES.size() > PROBE_MAX) {
                    Iterator<String> it = PROBES.keySet().iterator();
                    if (it.hasNext()) {
                        String oldest = it.next();
                        victim = PROBES.get(oldest);
                        it.remove();
                        PROBE_SEEN.remove(oldest);
                        Trace.debug("[P2P] Ping session pool over the cap, evicted {}", oldest);
                    }
                }
            }
        }
        if (victim != null) closeQuietly(victim);

        int port = j.localPort();
        if (port <= 0) {
            try {
                port = j.start(want);
            } catch (IOException e) {
                synchronized (PROBE_LOCK) { PROBES.remove(t, j); }
                closeQuietly(j);
                throw e;
            }
        }

        long deadline = System.currentTimeMillis() + 2_000;
        while (!j.ready() && !j.closed() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        Trace.debug("[P2P] List ping port 127.0.0.1:{} ({} ready={})", port, t, j.ready());
        return port;
    }

    private static void reapIdleProbes() {
        long now = System.currentTimeMillis();
        java.util.List<Joining> dead = null;
        synchronized (PROBE_LOCK) {
            Iterator<java.util.Map.Entry<String, Joining>> it = PROBES.entrySet().iterator();
            while (it.hasNext()) {
                java.util.Map.Entry<String, Joining> e = it.next();
                Long seen = PROBE_SEEN.get(e.getKey());
                if (seen != null && now - seen < PROBE_IDLE_MS) continue;
                if (dead == null) dead = new java.util.ArrayList<>(2);
                dead.add(e.getValue());
                it.remove();
                PROBE_SEEN.remove(e.getKey());
            }
        }
        if (dead == null) return;
        for (Joining j : dead) closeQuietly(j);
        Trace.debug("[P2P] Reclaimed {} idle ping sessions", dead.size());
    }

    private static void closeQuietly(Joining j) {
        try {
            j.close();
        } catch (Throwable ignored) {
        }
    }

    public static synchronized void host(String relayHost, int relayPort, String roomName, int mcPort)
            throws Exception {
        stopRoom();
        Hosting h = new Hosting(relayHost, relayPort, roomName, mcPort);
        h.start();
        room = h;
    }

    public static synchronized String rebind(String newRoom) throws Exception {
        Hosting current = room;
        if (current == null) throw new IllegalStateException("Not hosting right now, cannot change the room id");
        String host = current.relayHost();
        int port = current.relayPort();
        int mc = current.mcPort();
        stopRoom();
        host(host, port, newRoom, mc);
        Hosting after = room;
        return after == null ? newRoom : after.room();
    }

    public static synchronized void stopRoom() {
        Hosting h = room;
        room = null;
        if (h != null) h.close();
    }

    public static Hosting room() { return room; }

    public static Status snapshot() {
        Joining j = main;
        if (j == null) return Status.IDLE;
        var c = j.connection();
        if (c == null) return new Status("connecting", -1, -1, true);
        return new Status(c.routeLabel(), c.routeRtt(), c.directRtt(), true);
    }

    public static RouteMode mode() { return Config.mode; }

    private static volatile java.util.function.BiConsumer<String, Integer> switchNotice;

    public static void setSwitchNotice(java.util.function.BiConsumer<String, Integer> h) {
        switchNotice = h;
    }

    static void notifySwitch(String label, int rtt) {
        java.util.function.BiConsumer<String, Integer> h = switchNotice;
        if (h == null) return;
        try {
            h.accept(label, rtt);
        } catch (Throwable ignored) {
        }
    }

    public static synchronized RouteMode setMode(RouteMode m) {
        if (m == null) return Config.mode;
        Config.saveMode(m);
        Joining j = main;
        if (j != null) j.setMode(m);
        return m;
    }

    public static dev.p2pmod.core.Connection.LinkView link() {
        Joining j = main;
        if (j == null) return null;
        var c = j.connection();
        return c == null ? null : c.linkView();
    }

    public static String describe() {
        Joining j = main;
        if (j == null) return "link=none";
        var c = j.connection();
        return c == null ? "link=none" : c.describe();
    }

    public static String diagnose() {
        Joining j = main;
        if (j == null && room == null) return "no room";
        StringBuilder sb = new StringBuilder();
        if (room != null) {
            int clients = room.clientCount();
            sb.append("room[").append(room.room()).append("] clients[").append(clients).append(']');
            if (clients > 0) {
                int direct = room.directClients();
                sb.append(" direct[").append(direct).append("] relay[").append(clients - direct).append(']');
            }
            room.inspect();
        }
        if (j != null) {
            if (sb.length() > 0) sb.append(' ');
            var c = j.connection();
            sb.append("client[").append(c == null ? "connecting" : c.describe()).append(']');
        }
        return sb.toString();
    }

    public static synchronized void shutdownAll() {
        java.util.List<Joining> probes;
        synchronized (PROBE_LOCK) {
            probes = new java.util.ArrayList<>(PROBES.values());
            PROBES.clear();
            PROBE_SEEN.clear();
        }
        for (Joining p : probes) closeQuietly(p);
        try {
            leave();
        } catch (Throwable t) {
            Trace.debug("Closing the client session failed: {}", t.toString());
        }
        try {
            stopRoom();
        } catch (Throwable t) {
            Trace.debug("Closing the room failed: {}", t.toString());
        }
    }
}
