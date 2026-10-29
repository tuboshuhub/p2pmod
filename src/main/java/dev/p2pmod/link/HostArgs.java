package dev.p2pmod.link;

import dev.p2pmod.core.HostPort;
import dev.p2pmod.core.RoomId;
import dev.p2pmod.support.Config;

public final class HostArgs {

    private HostArgs() {}

    public record Parsed(String relayHost, int relayPort, String room) {}

    public static Parsed parse(String raw) {
        String[] parts = (raw == null || raw.isBlank())
                ? new String[0]
                : raw.trim().split("\\s+");

        if (parts.length > 3) {
            throw new IllegalArgumentException(
                    "Too many arguments (at most \"address port room\"): " + raw.trim());
        }

        if (parts.length == 1 && RoomId.isValid(parts[0])) {
            return new Parsed(null, Config.relayPort, parts[0]);
        }

        String host = parts.length >= 1 ? parts[0] : null;
        int port = Config.relayPort;
        String room = null;
        boolean portKnown = false;

        if (host != null) {

            HostPort.Split split = HostPort.split(host, port);
            host = split.host();
            if (split.explicit()) {
                port = split.port();
                portKnown = true;
            }
        }

        for (int i = 1; i < parts.length; i++) {
            String t = parts[i];
            boolean roomLike = RoomId.isValid(t);
            boolean portLike = isPort(t);

            if (!roomLike && !portLike) {
                throw new IllegalArgumentException("Unrecognised \"" + t
                        + "\": a port must be a number from 1-65535, a room id is 4 alphanumeric characters (case sensitive)");
            }
            if (portLike && !portKnown) {
                port = Integer.parseInt(t);
                portKnown = true;
            } else if (roomLike && room == null) {
                room = t;
            } else {
                throw new IllegalArgumentException("Redundant argument \"" + t + "\": port already set to "
                        + port + (room == null ? "" : " and room id " + room));
            }
        }
        return new Parsed(host, port, room);
    }

    private static boolean isPort(String s) {
        if (s == null || s.isEmpty() || s.length() > 5) return false;
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) return false;
        }
        int v = Integer.parseInt(s);
        return v >= 1 && v <= 65535;
    }
}
