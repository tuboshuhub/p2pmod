package dev.p2pmod.mixin;

import dev.p2pmod.link.Portal;
import dev.p2pmod.support.Trace;
import net.minecraft.client.multiplayer.resolver.ServerAddress;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(ServerAddress.class)
public abstract class ServerAddressMixin {

    private static final String PREFIX = "p2p:";

    @Inject(method = "isValidAddress", at = @At("HEAD"), cancellable = true)
    private static void p2pmod$acceptP2p(String input, CallbackInfoReturnable<Boolean> cir) {
        String target = extract(input);
        if (target != null) cir.setReturnValue(!target.isEmpty());
    }

    @Inject(method = "parseString", at = @At("HEAD"), cancellable = true)
    private static void p2pmod$redirect(String input, CallbackInfoReturnable<ServerAddress> cir) {
        String target = extract(input);
        if (target == null) return;

        String thread = Thread.currentThread().getName();
        if (thread != null && thread.startsWith("Server Pinger")) {

            try {
                int port = Portal.pingStatus(target);
                Trace.info("[P2P] Address parse [list ping] {} -> 127.0.0.1:{}", target, port);
                cir.setReturnValue(new ServerAddress("127.0.0.1", port));
            } catch (Exception e) {
                Trace.debug("[P2P] Creating the list ping session failed: {}", e.toString());
                cir.setReturnValue(new ServerAddress("127.0.0.1", 1));
            }
            return;
        }

        long t0 = System.currentTimeMillis();
        try {
            int port = Portal.join(target);
            Trace.info("[P2P] Address parse [play session] {} -> 127.0.0.1:{} (took {}ms, thread {})",
                    target, port, System.currentTimeMillis() - t0, thread);
            cir.setReturnValue(new ServerAddress("127.0.0.1", port));
        } catch (Exception e) {
            Trace.error("[P2P] Establishing the tunnel failed: {}", e.toString());
            cir.setReturnValue(new ServerAddress("127.0.0.1", 1));
        }
        long spent = System.currentTimeMillis() - t0;
        if (spent > 500) {
            Trace.warn("[P2P] Address parsing blocked the calling thread for {}ms (thread {}) -- tunnel setup should not block that long",
                    spent, thread);
        }
    }

    private static String extract(String input) {
        if (input == null) return null;
        String t = input.trim();
        return t.startsWith(PREFIX) ? t.substring(PREFIX.length()).trim() : null;
    }
}
