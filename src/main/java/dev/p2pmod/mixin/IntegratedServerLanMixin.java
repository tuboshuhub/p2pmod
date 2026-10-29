package dev.p2pmod.mixin;

import dev.p2pmod.edge.LanRoom;
import net.minecraft.client.server.IntegratedServer;
import net.minecraft.world.level.GameType;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(IntegratedServer.class)
public abstract class IntegratedServerLanMixin {

    @Inject(method = "publishServer", at = @At("RETURN"))
    private void p2pmod$afterPublish(GameType gameType, boolean allowCommands, int port,
                                     CallbackInfoReturnable<Boolean> cir) {
        Boolean published = cir.getReturnValue();
        if (published == null || !published) return;
        LanRoom.onPublished(port);
    }
}
