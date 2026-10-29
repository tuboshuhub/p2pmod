package dev.p2pmod.command;

import com.mojang.brigadier.arguments.IntegerArgumentType;
import com.mojang.brigadier.arguments.StringArgumentType;
import dev.p2pmod.P2PTunnel;
import dev.p2pmod.core.Connection;
import dev.p2pmod.link.Portal;
import dev.p2pmod.support.Config;
import dev.p2pmod.support.RouteMode;
import dev.p2pmod.support.Scheduler;
import net.minecraft.ChatFormatting;
import net.minecraft.client.Minecraft;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.commands.SharedSuggestionProvider;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.MutableComponent;
import net.neoforged.api.distmarker.Dist;
import net.neoforged.bus.api.SubscribeEvent;
import net.neoforged.fml.common.EventBusSubscriber;
import net.neoforged.neoforge.client.event.RegisterClientCommandsEvent;

@EventBusSubscriber(modid = P2PTunnel.MODID, value = Dist.CLIENT)
public final class ClientP2PCommands {

    private ClientP2PCommands() {}

    private static final String RELAY = "relay";
    private static final String DIRECT = "direct";

    private static final int DEFAULT_SECONDS = 5;

    private static volatile long autoIntervalMs;
    private static long autoLastMs;

    private static final long MODE_ECHO_MS = 1_500;
    private static final long NOTICE_DEDUPE_MS = 3_000;
    private static volatile long modeChangeMs;
    private static long noticeAtMs;
    private static String noticeKey = "";

    static {

        Scheduler.register(ClientP2PCommands::autoTick);
    }

    public static void install() {
        Portal.setSwitchNotice(ClientP2PCommands::noticeSwitch);
    }

    @SubscribeEvent
    public static void onRegister(RegisterClientCommandsEvent event) {
        event.getDispatcher().register(
            Commands.literal("p2p")
                .then(Commands.literal("status")
                    .executes(ctx -> {
                        ctx.getSource().sendSuccess(ClientP2PCommands::status, false);
                        return 1;
                    })
                    .then(Commands.literal("start")
                        .executes(ctx -> startAuto(ctx.getSource(), DEFAULT_SECONDS))
                        .then(Commands.argument("seconds", IntegerArgumentType.integer(1, 3600))
                            .executes(ctx -> startAuto(ctx.getSource(),
                                    IntegerArgumentType.getInteger(ctx, "seconds")))))
                    .then(Commands.literal("stop").executes(ctx -> stopAuto(ctx.getSource()))))

                .then(Commands.literal("mode")
                    .then(Commands.argument("mode", StringArgumentType.word())
                        .suggests((ctx, b) -> SharedSuggestionProvider.suggest(new String[]{"auto", "relay"}, b))
                        .executes(ctx -> setMode(ctx.getSource(), StringArgumentType.getString(ctx, "mode")))))
        );
    }

    private static Component status() {
        MutableComponent out = line();
        out.append(modeTag());
        Connection.LinkView v = Portal.link();
        if (v == null) {
            return out.append(text(" ", ChatFormatting.WHITE))
                    .append(tr("p2pmod.status.link", ChatFormatting.WHITE))
                    .append(tag("none"));
        }
        boolean onRelay = RELAY.equals(v.route());
        boolean onDirect = DIRECT.equals(v.route());
        out.append(text(" ", ChatFormatting.WHITE)).append(tr("p2pmod.status.link", ChatFormatting.WHITE));
        out.append(tag(v.route()));
        if (v.rtt() >= 0) {
            out.append(text(" ", ChatFormatting.WHITE)).append(tr("p2pmod.unit.ms", ChatFormatting.WHITE, v.rtt()));
        }

        out.append(text(" ", ChatFormatting.WHITE))
                .append(tr("p2pmod.status.loss", ChatFormatting.WHITE))
                .append(text(" ", ChatFormatting.WHITE));
        out.append(text(pct(v.upLossTenths()), lossColour(v.upLossTenths())));
        out.append(text(" ", ChatFormatting.WHITE));
        out.append(text(pct(v.downLossTenths()), lossColour(v.downLossTenths())));

        if (!onRelay) out.append(text(" ", ChatFormatting.WHITE)).append(side(RELAY, v.relay()));
        if (!onDirect) out.append(text(" ", ChatFormatting.WHITE)).append(side(DIRECT, v.direct()));
        return out;
    }

    private static Component side(String name, Connection.PathView p) {
        MutableComponent c = tag(name);
        if (p == null) return c.append(text(" -", ChatFormatting.DARK_GRAY));
        if (!p.live()) {
            return c.append(text(" ", ChatFormatting.WHITE))
                    .append(tr("p2pmod.path.down", ChatFormatting.WHITE));
        }
        if (p.rtt() <= 0) {
            return c.append(text(" ", ChatFormatting.WHITE))
                    .append(tr("p2pmod.path.up", ChatFormatting.WHITE));
        }
        return c.append(text(" ", ChatFormatting.WHITE))
                .append(tr("p2pmod.unit.ms", ChatFormatting.WHITE, p.rtt()));
    }

    private static MutableComponent tag(String path) {

        String key = "none".equals(path) ? "p2pmod.path.none" : "p2pmod.path." + path;
        ChatFormatting c = colour(path);
        return text("[", c).append(tr(key, c)).append(text("]", c));
    }

    private static MutableComponent modeTag() {

        return tr("p2pmod.status.mode", ChatFormatting.WHITE)
                .append(text("[" + Config.mode.id + "]", ChatFormatting.AQUA));
    }

    private static int startAuto(CommandSourceStack src, int seconds) {
        autoIntervalMs = seconds * 1000L;
        autoLastMs = 0;
        src.sendSuccess(() -> line().append(tr("p2pmod.status.repeat_on", ChatFormatting.WHITE, seconds)), false);
        return 1;
    }

    private static int stopAuto(CommandSourceStack src) {
        autoIntervalMs = 0;
        src.sendSuccess(() -> line().append(tr("p2pmod.status.repeat_off", ChatFormatting.WHITE)), false);
        return 1;
    }

    private static void autoTick() {
        long interval = autoIntervalMs;
        if (interval <= 0) return;
        long now = System.currentTimeMillis();
        if (now - autoLastMs < interval) return;
        autoLastMs = now;
        Minecraft mc = Minecraft.getInstance();
        if (mc == null) return;
        mc.execute(() -> {
            if (mc.player != null) mc.player.displayClientMessage(status(), false);
        });
    }

    private static void noticeSwitch(String label, int rtt) {
        long now = System.currentTimeMillis();

        if (now - modeChangeMs < MODE_ECHO_MS) return;

        String key = label;
        if (key.equals(noticeKey) && now - noticeAtMs < NOTICE_DEDUPE_MS) return;
        showNotice(label, rtt);
    }

    private static void showNotice(String label, int rtt) {
        noticeKey = label;
        noticeAtMs = System.currentTimeMillis();
        Minecraft mc = Minecraft.getInstance();
        if (mc == null) return;

        mc.execute(() -> {
            if (mc.player != null) mc.player.displayClientMessage(switchLine(label, rtt), false);
        });
    }

    private static MutableComponent switchLine(String label, int rtt) {
        MutableComponent msg = line().append(tr("p2pmod.status.link", ChatFormatting.WHITE)).append(tag(label));
        if (rtt >= 0) msg.append(text(" " + rtt + "ms", ChatFormatting.WHITE));
        return msg;
    }

    private static int setMode(CommandSourceStack src, String raw) {
        RouteMode mode = RouteMode.parse(raw);
        if (mode == null) {
            src.sendFailure(line().append(tr("p2pmod.mode.unknown", ChatFormatting.RED, raw)));
            return 0;
        }

        Connection.LinkView before = Portal.link();
        String beforeRoute = before == null ? "none" : before.route();
        modeChangeMs = System.currentTimeMillis();
        Portal.setMode(mode);
        Connection.LinkView v = Portal.link();
        if (v != null && !"none".equals(v.route()) && !v.route().equals(beforeRoute)) {

            showNotice(v.route(), v.rtt());
        }

        return 1;
    }

    private static String pct(int tenths) {
        return tenths >= 10 ? (tenths / 10) + "%" : String.format(java.util.Locale.ROOT, "%.1f%%", tenths / 10.0);
    }

    private static ChatFormatting lossColour(int tenths) {
        if (tenths < 10) return ChatFormatting.GREEN;
        if (tenths <= 50) return ChatFormatting.YELLOW;
        return ChatFormatting.RED;
    }

    private static MutableComponent line() {
        return text("[P2P] ", ChatFormatting.WHITE);
    }

    private static MutableComponent text(String s, ChatFormatting colour) {
        return Component.literal(s).withStyle(colour);
    }

    private static MutableComponent tr(String key, ChatFormatting colour, Object... args) {
        return Component.translatable(key, args).withStyle(colour);
    }

    private static ChatFormatting colour(String path) {
        return switch (path) {
            case DIRECT -> ChatFormatting.GREEN;
            case RELAY -> ChatFormatting.YELLOW;
            default -> ChatFormatting.WHITE;
        };
    }
}
