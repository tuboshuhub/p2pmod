package dev.p2pmod.support;

public enum RouteMode {

    AUTO("auto"),
    RELAY("relay");

    public final String id;

    RouteMode(String id) {
        this.id = id;
    }

    public static RouteMode parse(String raw) {
        if (raw == null) return null;
        String s = raw.trim().toLowerCase(java.util.Locale.ROOT);
        return switch (s) {
            case "auto", "automatic", "" -> AUTO;
            case "relay", "relayed", "prefer-relay" -> RELAY;
            default -> null;
        };
    }

    public boolean preferRelay() {
        return this == RELAY;
    }

    public static RouteMode of(String raw) {
        RouteMode m = parse(raw);
        return m == null ? AUTO : m;
    }
}
