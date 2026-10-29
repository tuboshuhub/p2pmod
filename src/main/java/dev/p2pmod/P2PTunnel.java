package dev.p2pmod;

import dev.p2pmod.command.P2PCommands;
import dev.p2pmod.link.AutoRoom;
import dev.p2pmod.link.Portal;
import dev.p2pmod.link.RouteBroadcast;
import dev.p2pmod.net.RouteCache;
import dev.p2pmod.net.RoutePayload;
import dev.p2pmod.support.Config;
import dev.p2pmod.support.Trace;
import net.neoforged.api.distmarker.Dist;
import net.neoforged.bus.api.IEventBus;
import net.neoforged.fml.common.Mod;
import net.neoforged.fml.loading.FMLEnvironment;
import net.neoforged.neoforge.common.NeoForge;
import net.neoforged.neoforge.event.RegisterCommandsEvent;
import net.neoforged.neoforge.event.server.ServerStartedEvent;
import net.neoforged.neoforge.event.server.ServerStoppedEvent;
import net.neoforged.neoforge.event.tick.ServerTickEvent;
import net.neoforged.neoforge.network.event.RegisterPayloadHandlersEvent;
import net.neoforged.neoforge.network.registration.PayloadRegistrar;

@Mod(P2PTunnel.MODID)
public class P2PTunnel {

    public static final String MODID = "p2pmod";

    public P2PTunnel(IEventBus modBus) {
        Config.load();

        modBus.addListener((RegisterPayloadHandlersEvent e) -> {
            PayloadRegistrar reg = e.registrar("1");
            reg.playToClient(RoutePayload.TYPE, RoutePayload.CODEC,
                    (payload, ctx) -> ctx.enqueueWork(() -> {
                        try {
                            RouteCache.accept(payload);
                        } catch (Throwable t) {

                            Trace.debug("[P2P] Receiving the route table failed: {}", t.toString());
                        }
                    }));
        });

        NeoForge.EVENT_BUS.addListener((RegisterCommandsEvent e) ->
                P2PCommands.registerServer(e.getDispatcher()));

        NeoForge.EVENT_BUS.addListener((ServerTickEvent.Post e) -> RouteBroadcast.tick(e.getServer()));

        NeoForge.EVENT_BUS.addListener((ServerStartedEvent e) -> {

            if (FMLEnvironment.dist == Dist.DEDICATED_SERVER) {
                AutoRoom.onServerReady(e.getServer().getPort());
            }
        });

        NeoForge.EVENT_BUS.addListener((ServerStoppedEvent e) -> {
            AutoRoom.onServerStopped();
            Portal.shutdownAll();
        });

        if (FMLEnvironment.dist == Dist.DEDICATED_SERVER) {
            AutoRoom.bootstrapRelay();
        }

        Trace.info("[P2P] Tunnel mod loaded; connect with p2p:<room>@<address>[:port]");
    }

}
