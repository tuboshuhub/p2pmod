package dev.p2pmod.link;

import dev.p2pmod.net.RoutePayload;
import dev.p2pmod.support.Trace;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.neoforged.neoforge.network.PacketDistributor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class RouteBroadcast {

    private RouteBroadcast() {}

    private static final long INTERVAL_MS = 1_000;

    private static long lastMs;

    public static void tick(MinecraftServer server) {
        if (server == null) return;
        long now = System.currentTimeMillis();
        if (now - lastMs < INTERVAL_MS) return;
        lastMs = now;

        Hosting room = Portal.room();
        if (room == null) return;
        Map<Integer, Hosting.Route> routes = room.routesByLocalPort();
        if (routes.isEmpty()) return;

        List<RoutePayload.Entry> entries = new ArrayList<>();
        for (ServerPlayer p : server.getPlayerList().getPlayers()) {
            SocketAddress remote = p.connection.getRemoteAddress();
            if (!(remote instanceof InetSocketAddress isa)) continue;
            Hosting.Route r = routes.get(isa.getPort());

            if (r == null) continue;
            entries.add(new RoutePayload.Entry(p.getUUID(), r.direct(), r.rtt()));
        }
        if (entries.isEmpty()) return;

        try {
            PacketDistributor.sendToAllPlayers(new RoutePayload(List.copyOf(entries)));
        } catch (Throwable t) {

            Trace.debug("[P2P] Route table broadcast failed: {}", t.toString());
        }
    }
}
