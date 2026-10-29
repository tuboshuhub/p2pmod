package dev.p2pmod.mixin;

import net.minecraft.client.multiplayer.ClientCommonPacketListenerImpl;
import net.minecraft.network.protocol.common.ClientboundKeepAlivePacket;
import net.minecraft.network.protocol.common.ServerboundKeepAlivePacket;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(ClientCommonPacketListenerImpl.class)
public abstract class KeepAliveReplyMixin {

    @Inject(method = "handleKeepAlive", at = @At("HEAD"), cancellable = true)
    private void p2pmod$replyNow(ClientboundKeepAlivePacket packet, CallbackInfo ci) {
        ClientCommonPacketListenerImpl self = (ClientCommonPacketListenerImpl) (Object) this;
        self.send(new ServerboundKeepAlivePacket(packet.getId()));
        ci.cancel();
    }
}
