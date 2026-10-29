package dev.p2pmod.mixin;

import dev.p2pmod.link.Portal;
import dev.p2pmod.net.RouteCache;
import dev.p2pmod.net.RoutePayload;
import net.minecraft.ChatFormatting;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.GuiGraphics;
import net.minecraft.client.gui.components.PlayerTabOverlay;
import net.minecraft.client.multiplayer.PlayerInfo;
import net.minecraft.network.chat.Component;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.util.UUID;

@Mixin(PlayerTabOverlay.class)
public abstract class PlayerTabOverlayMixin {

    @Inject(method = "getNameForDisplay", at = @At("RETURN"), cancellable = true)
    private void p2pmod$decorate(PlayerInfo info, CallbackInfoReturnable<Component> cir) {
        Minecraft mc = Minecraft.getInstance();
        if (mc.player == null || info == null) return;
        Component base = cir.getReturnValue();
        if (base == null) return;

        UUID id = info.getProfile().getId();
        Component tag = id.equals(mc.player.getUUID()) ? ownTag() : otherTag(info, id);
        if (tag == null) return;
        cir.setReturnValue(base.copy().append(tag));
    }

    private static Component ownTag() {
        Portal.Status st = Portal.snapshot();
        if (!st.active() || st.rtt() <= 0) return null;
        return tag(st.route().startsWith("direct"), st.rtt());
    }

    private static Component otherTag(PlayerInfo info, UUID id) {
        RoutePayload.Entry e = RouteCache.lookup(id);
        if (e != null && e.rtt() > 0) return tag(e.direct(), e.rtt());

        int latency = info.getLatency();
        if (latency <= 0) return null;
        return Component.literal(" ")
                .append(Component.translatable("p2pmod.unit.ms", latency))
                .withStyle(ChatFormatting.GRAY);
    }

    private static Component tag(boolean direct, int rtt) {
        ChatFormatting colour = direct ? ChatFormatting.GREEN : ChatFormatting.YELLOW;
        return Component.literal(" [")
                .append(Component.translatable(direct ? "p2pmod.path.direct" : "p2pmod.path.relay"))
                .append(Component.literal("] "))
                .append(Component.translatable("p2pmod.unit.ms", rtt))
                .withStyle(colour);
    }

    @Inject(method = "renderPingIcon", at = @At("HEAD"), cancellable = true)
    private void p2pmod$hidePingBars(GuiGraphics guiGraphics, int x, int y, int width,
                                     PlayerInfo info, CallbackInfo ci) {
        ci.cancel();
    }
}
