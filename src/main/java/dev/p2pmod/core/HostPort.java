package dev.p2pmod.core;

public final class HostPort {

    private HostPort() {}

    public record Split(String host, int port, boolean explicit) {}

    public static Split split(String raw, int fallback) {
        if (raw == null) return new Split("", fallback, false);
        String s = raw.trim();
        if (s.isEmpty()) return new Split("", fallback, false);

        if (s.startsWith("[")) {
            int close = s.indexOf(']');
            if (close > 0) {
                String host = s.substring(1, close);
                String rest = s.substring(close + 1);
                if (rest.startsWith(":") && rest.length() > 1) {
                    int p = parsePort(rest.substring(1));
                    if (p > 0) return new Split(host, p, true);
                }
                return new Split(host, fallback, false);
            }

        }

        int colons = 0;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) == ':') colons++;
        if (colons > 1) return new Split(s, fallback, false);

        int colon = s.indexOf(':');
        if (colon > 0 && colon < s.length() - 1) {
            int p = parsePort(s.substring(colon + 1));
            if (p > 0) return new Split(s.substring(0, colon), p, true);
        }
        return new Split(s, fallback, false);
    }

    private static int parsePort(String s) {
        if (s.isEmpty() || s.length() > 5) return -1;
        for (int i = 0; i < s.length(); i++) if (!Character.isDigit(s.charAt(i))) return -1;
        int v = Integer.parseInt(s);
        return v >= 1 && v <= 65535 ? v : -1;
    }
}
