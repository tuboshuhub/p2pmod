package dev.p2pmod.net;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload.Type;
import net.minecraft.resources.ResourceLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public record RoutePayload(List<Entry> entries) implements CustomPacketPayload {

    public record Entry(UUID player, boolean direct, int rtt) {}

    public static final Type<RoutePayload> TYPE =
            new Type<>(ResourceLocation.fromNamespaceAndPath("p2pmod", "routes"));

    public static final StreamCodec<RegistryFriendlyByteBuf, RoutePayload> CODEC = StreamCodec.of(
            (buf, payload) -> {
                buf.writeVarInt(payload.entries().size());
                for (Entry e : payload.entries()) {
                    buf.writeUUID(e.player());
                    buf.writeBoolean(e.direct());
                    buf.writeVarInt(e.rtt());
                }
            },
            buf -> {
                int n = buf.readVarInt();

                if (n < 0 || n > 4096) throw new IllegalArgumentException("Unexpected route table entry count: " + n);
                List<Entry> list = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    UUID id = buf.readUUID();
                    boolean direct = buf.readBoolean();
                    int rtt = buf.readVarInt();
                    list.add(new Entry(id, direct, rtt));
                }
                return new RoutePayload(List.copyOf(list));
            });

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}
