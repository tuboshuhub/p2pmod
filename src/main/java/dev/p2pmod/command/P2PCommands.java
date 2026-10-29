package dev.p2pmod.command;

import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.arguments.StringArgumentType;
import dev.p2pmod.core.RoomId;
import dev.p2pmod.link.AutoRoom;
import dev.p2pmod.link.HostArgs;
import dev.p2pmod.link.Hosting;
import dev.p2pmod.link.RoomTarget;
import dev.p2pmod.link.Portal;
import dev.p2pmod.support.Config;
import dev.p2pmod.support.Trace;
import net.minecraft.ChatFormatting;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.MutableComponent;

public final class P2PCommands {

    private P2PCommands() {}

    public static void registerServer(CommandDispatcher<CommandSourceStack> dispatcher) {
        dispatcher.register(
            Commands.literal("p2p")
                .then(Commands.literal("status").executes(ctx -> {
                    ctx.getSource().sendSuccess(
                            () -> prefix().append(statusLine()).withStyle(ChatFormatting.WHITE), false);
                    return 1;
                }))
                .then(Commands.literal("host")

                    .requires(src -> src.hasPermission(2))
                    .then(Commands.literal("start")
                        .executes(ctx -> start(ctx.getSource(), ""))
                        .then(Commands.argument("args", StringArgumentType.greedyString())
                            .executes(ctx -> start(ctx.getSource(),
                                    StringArgumentType.getString(ctx, "args")))))
                    .then(Commands.literal("room")
                        .then(Commands.argument("room", StringArgumentType.word())
                            .executes(ctx -> rebind(ctx.getSource(),
                                    StringArgumentType.getString(ctx, "room"))))))
                .then(Commands.literal("close")
                    .requires(src -> src.hasPermission(2))
                    .executes(ctx -> {
                    if (!requireOp(ctx.getSource())) return 0;
                    Portal.stopRoom();
                    ctx.getSource().sendSuccess(
                            () -> prefix().append(Component.translatable("p2pmod.host.closed"))
                                    .withStyle(ChatFormatting.YELLOW),
                            true);
                    return 1;
                }))
        );
    }

    private static MutableComponent prefix() {
        return Component.literal("[P2P] ");
    }

    private static Component statusLine() {
        Hosting h = Portal.room();
        if (h == null) return Component.literal(Portal.diagnose());
        int clients = h.clientCount();
        MutableComponent line = Component.translatable("p2pmod.server.room", h.room(), clients);
        if (clients > 0) {
            int direct = h.directClients();
            line.append(Component.translatable("p2pmod.server.paths", direct, clients - direct));
        }
        h.inspect();
        return line;
    }

    private static boolean requireOp(CommandSourceStack src) {
        if (src.hasPermission(2)) return true;
        src.sendFailure(prefix().append(Component.translatable("p2pmod.host.op_only"))
                .withStyle(ChatFormatting.RED));
        return false;
    }

    private static int rebind(CommandSourceStack src, String room) {
        if (!requireOp(src)) return 0;
        if (Portal.room() == null) {
            src.sendFailure(prefix().append(Component.translatable("p2pmod.host.no_room"))
                    .withStyle(ChatFormatting.RED));
            return 0;
        }
        String actual;
        try {
            actual = Portal.rebind(room);
        } catch (Exception e) {
            src.sendFailure(prefix().append(Component.translatable("p2pmod.host.rename_failed", e.getMessage()))
                    .withStyle(ChatFormatting.RED));
            return 0;
        }
        Component hint = actual.equals(room) ? Component.empty()
                : Component.translatable("p2pmod.host.reassigned");
        Trace.info("[P2P] Room id changed to {}", actual);

        String target = RoomTarget.target(actual);
        src.sendSuccess(() -> prefix()
                        .append(Component.translatable("p2pmod.host.room", actual, hint, target))
                        .append(Component.translatable("p2pmod.host.reconnect"))
                        .withStyle(ChatFormatting.GREEN), true);
        return 1;
    }

    private static int start(CommandSourceStack src, String args) {
        if (!requireOp(src)) return 0;
        HostArgs.Parsed a;
        try {
            a = HostArgs.parse(args);
        } catch (IllegalArgumentException e) {

            src.sendFailure(Component.literal("[P2P] " + e.getMessage())
                    .withStyle(ChatFormatting.RED));
            return 0;
        }
        boolean embedded = a.relayHost() == null || a.relayHost().isBlank();
        String relayHost = embedded ? "127.0.0.1" : a.relayHost();
        String room = (a.room() == null || a.room().isBlank())
                ? RoomId.normalize(Config.defaultRoom) : a.room();

        try {

            AutoRoom.bootstrapRelay();
            Portal.host(relayHost, a.relayPort(), room, src.getServer().getPort());
        } catch (Exception e) {
            src.sendFailure(prefix().append(Component.translatable("p2pmod.host.failed", e.getMessage()))
                    .withStyle(ChatFormatting.RED));
            return 0;
        }

        String actual = Portal.room() == null ? room : Portal.room().room();
        Component hint = actual.equals(room) ? Component.empty()
                : Component.translatable("p2pmod.host.reassigned");
        Trace.info("[P2P] Registered {} relay={}:{}", actual, relayHost, a.relayPort());

        String target = RoomTarget.target(actual);
        src.sendSuccess(() -> prefix()
                        .append(Component.translatable("p2pmod.host.registered", actual, hint, target))
                        .withStyle(ChatFormatting.GREEN), true);
        return 1;
    }
}
