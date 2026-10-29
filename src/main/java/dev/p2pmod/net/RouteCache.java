package dev.p2pmod.net;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class RouteCache {

    private RouteCache() {}

    private static final long TTL_MS = 10_000;

    private record Seen(boolean direct, int rtt, long atMs) {}

    private static volatile Map<UUID, Seen> table = Map.of();

    public static void accept(RoutePayload payload) {
        long now = System.currentTimeMillis();
        Map<UUID, Seen> next = new ConcurrentHashMap<>(Math.max(4, payload.entries().size() * 2));
        for (RoutePayload.Entry e : payload.entries()) {
            next.put(e.player(), new Seen(e.direct(), e.rtt(), now));
        }
        table = next;
    }

    public static RoutePayload.Entry lookup(UUID player) {
        Seen s = table.get(player);
        if (s == null) return null;
        if (System.currentTimeMillis() - s.atMs() > TTL_MS) return null;
        return new RoutePayload.Entry(player, s.direct(), s.rtt());
    }

    public static void clear() {
        table = Map.of();
    }
}
