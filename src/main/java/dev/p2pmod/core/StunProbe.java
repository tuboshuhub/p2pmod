package dev.p2pmod.core;

import dev.p2pmod.support.Trace;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class StunProbe {

    private StunProbe() {}

    public enum NatType {

        CONE("cone"),

        SYMMETRIC("symmetric"),

        UNKNOWN("unknown");

        public final String label;
        NatType(String label) { this.label = label; }
    }

    public record Result(InetSocketAddress mapped, NatType type) {}

    private static final String[] SERVERS = {
        "stun.l.google.com:19302",
        "stun1.l.google.com:19302",
        "stun.xten.com:3478",
        "stun.voipbuster.com:3478",
        "stun.miwifi.com:3478",
    };

    private static final long TIMEOUT_MS = 2_000;
    private static final int MAGIC = 0x2112A442;

    public static String[] defaultServers() {
        return SERVERS.clone();
    }

    public static Result probe(Endpoint endpoint) {
        return probe(endpoint, null);
    }

    public static Result probe(Endpoint endpoint, String[] servers) {
        String[] list = (servers == null || servers.length == 0) ? SERVERS : servers;
        InetSocketAddress first = null;
        String firstServer = null;
        for (String s : list) {
            InetSocketAddress mapped = ask(endpoint, s);
            if (mapped == null) continue;
            if (first == null) {
                first = mapped;
                firstServer = s;
                continue;
            }

            boolean samePort = mapped.getPort() == first.getPort()
                    && mapped.getAddress().equals(first.getAddress());
            NatType type = samePort ? NatType.CONE : NatType.SYMMETRIC;
            log(first, type, firstServer + " / " + s);
            return new Result(first, type);
        }
        if (first != null) {
            log(first, NatType.UNKNOWN, firstServer);
            return new Result(first, NatType.UNKNOWN);
        }
        Trace.debug("[P2P] All STUN probes failed (public servers unreachable); direct paths unlikely, relay only");
        return null;
    }

    private static void log(InetSocketAddress mapped, NatType type, String via) {
        Trace.info("[P2P] STUN probe done: public egress={} local NAT type={} (via {})", mapped, type.label, via);
        Trace.info("[P2P] Direct connectivity depends on both NAT types: traversal essentially fails only when "
                + "both sides are symmetric; trust the actual path switch, not this prediction");
    }

    private static InetSocketAddress ask(Endpoint endpoint, String server) {

        HostPort.Split split = HostPort.split(server, 3478);
        if (split.host().isEmpty()) return null;
        InetSocketAddress target;
        try {
            target = new InetSocketAddress(InetAddress.getByName(split.host()), split.port());
        } catch (Exception e) {
            return null;
        }

        byte[] txid = new byte[12];
        new java.security.SecureRandom().nextBytes(txid);
        byte[] req = request(txid);

        AtomicReference<byte[]> got = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        endpoint.registerStun(txid, data -> {
            got.set(data);
            latch.countDown();
        });
        try {
            endpoint.send(target, req);
            if (!latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) return null;
        } catch (Exception e) {
            return null;
        } finally {
            endpoint.unregisterStun(txid);
        }
        byte[] resp = got.get();
        return resp == null ? null : parse(resp, txid);
    }

    public static byte[] request(byte[] txid) {
        byte[] out = new byte[20];
        out[0] = 0x00;
        out[1] = 0x01;
        out[2] = 0x00;
        out[3] = 0x00;
        Bytes.putInt(out, 4, MAGIC);
        System.arraycopy(txid, 0, out, 8, 12);
        return out;
    }

    public static InetSocketAddress parse(byte[] resp, byte[] txid) {
        if (resp == null || resp.length < 20) return null;
        if ((resp[0] & 0xFF) != 0x01 || (resp[1] & 0xFF) != 0x01) return null;

        if (txid != null && txid.length == 12) {
            for (int i = 0; i < 12; i++) {
                if (resp[8 + i] != txid[i]) return null;
            }
        }
        int off = 20;
        while (off + 4 <= resp.length) {
            int type = ((resp[off] & 0xFF) << 8) | (resp[off + 1] & 0xFF);
            int len = ((resp[off + 2] & 0xFF) << 8) | (resp[off + 3] & 0xFF);
            if (off + 4 + len > resp.length) break;
            int value = off + 4;
            if (type == 0x0020 && len >= 8) {
                return decode(resp, value, txid, true);
            }
            if (type == 0x0001 && len >= 8) {
                return decode(resp, value, txid, false);
            }
            off = value + len + ((len % 4 == 0) ? 0 : (4 - len % 4));
        }
        return null;
    }

    private static InetSocketAddress decode(byte[] b, int v, byte[] txid, boolean xor) {
        if ((b[v + 1] & 0xFF) != 0x01) return null;
        int port = ((b[v + 2] & 0xFF) << 8) | (b[v + 3] & 0xFF);
        byte[] ip = new byte[4];
        System.arraycopy(b, v + 4, ip, 0, 4);
        if (xor) {
            port ^= (MAGIC >>> 16);
            for (int i = 0; i < 4; i++) {
                ip[i] ^= (byte) ((MAGIC >>> (24 - i * 8)) & 0xFF);
            }
        }
        try {
            return new InetSocketAddress(InetAddress.getByAddress(ip), port);
        } catch (Exception e) {
            return null;
        }
    }
}
