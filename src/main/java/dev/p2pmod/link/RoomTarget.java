package dev.p2pmod.link;

import dev.p2pmod.core.Signaling;
import dev.p2pmod.support.Config;

public final class RoomTarget {

    private RoomTarget() {}

    public static String address(Hosting h) {
        if (h == null) return "";
        String host = h.relayHost() == null || h.relayHost().isBlank() ? "127.0.0.1" : h.relayHost();
        int port = h.relayPort();
        boolean defaultPort = port == Signaling.DEFAULT_PORT;

        if (!Config.isLoopback(host)) {
            return defaultPort ? bracket(host) : bracket(host) + ":" + port;
        }
        String ip = h.publicIp();
        long deadline = System.currentTimeMillis() + 500;
        while (ip == null && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            ip = h.publicIp();
        }

        if (ip != null) return defaultPort ? bracket(ip) : bracket(ip) + ":" + port;
        return host + ":" + port;
    }

    public static String target(String room) {
        String addr = address(Portal.room());
        return addr.isEmpty() ? room : room + "@" + addr;
    }

    public static String bracket(String host) {
        return host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
    }
}
