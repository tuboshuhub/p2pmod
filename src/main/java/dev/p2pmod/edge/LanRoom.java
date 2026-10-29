package dev.p2pmod.edge;

import dev.p2pmod.link.AutoRoom;
import dev.p2pmod.link.RoomTarget;
import dev.p2pmod.support.Config;
import dev.p2pmod.support.Trace;
import dev.p2pmod.support.Workers;
import net.minecraft.client.Minecraft;
import net.minecraft.network.chat.Component;

public final class LanRoom {

    private LanRoom() {}

    public static void onPublished(int mcPort) {
        if (!Config.autoRoomOnLan) return;
        if (mcPort <= 0) return;
        Workers.run(() -> {
            String room;
            try {
                room = AutoRoom.onLanOpened(mcPort);
            } catch (Throwable t) {
                Trace.warn("[P2P] Could not open a room for the LAN world: {}", t.toString());
                String why = t.getMessage() == null ? t.toString() : t.getMessage();
                notice(Component.translatable("p2pmod.lan.failed", why));
                return;
            }
            if (room == null) return;
            String target = RoomTarget.target(room);
            Trace.info("[P2P] LAN room ready: {} (join with p2p:{})", room, target);
            notice(Component.translatable("p2pmod.lan.opened", room, target));
        });
    }

    private static void notice(Component message) {
        Minecraft mc = Minecraft.getInstance();
        if (mc == null) return;
        mc.execute(() -> {
            if (mc.player == null) return;
            mc.player.displayClientMessage(Component.literal("[P2P] ").append(message), false);
        });
    }
}
