package dev.p2pmod.support;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class Config {

    private Config() {}

    public static final Path FILE = Path.of("config", "p2pmod.properties");

    public static volatile String relayHost = "";
    public static volatile int relayPort = 34777;

    public static volatile boolean relayEmbedded = true;

    public static volatile int hostPunchPort = 34778;

    public static volatile boolean verbose = false;

    public static volatile boolean forceRelay = false;

    public static volatile String[] stunServers = new String[0];

    public static volatile int relayIdleTimeoutSec = 90;

    public static volatile boolean relayForward = true;

    public static long keepaliveIntervalMs() {
        return 10_000L;
    }

    public static volatile String defaultRoom = "";

    public static volatile boolean stunEnabled = true;

    public static volatile boolean acceptBeforeReady = true;

    public static volatile boolean autoRoomOnLan = true;

    public static volatile RouteMode mode = RouteMode.AUTO;

    public static volatile boolean fecEnabled = true;
    public static volatile int fecGroup = 10;
    public static volatile int fecParity = 2;

    public static void load() {
        try {
            Properties p = new Properties();
            if (Files.isRegularFile(FILE)) {
                try (InputStream in = Files.newInputStream(FILE)) {
                    p.load(new java.io.InputStreamReader(in, java.nio.charset.StandardCharsets.UTF_8));
                }
            } else {
                writeDefault();
                return;
            }
            relayHost = p.getProperty("relay.host", "").trim();
            relayPort = intOr(p.getProperty("relay.port"), 34777);
            relayEmbedded = boolOr(p.getProperty("relay.embedded"), true);
            hostPunchPort = intOr(p.getProperty("host.punchPort"), 34778);
            verbose = boolOr(p.getProperty("verbose"), false);
            forceRelay = boolOr(p.getProperty("forceRelay"), false);
            stunServers = splitServers(p.getProperty("stun.servers"));
            stunEnabled = boolOr(p.getProperty("stun.enabled"), true);
            relayIdleTimeoutSec = intOr(p.getProperty("relay.idleTimeoutSec"), 90);
            relayForward = boolOr(p.getProperty("relay.forward"), true);
            defaultRoom = p.getProperty("room", "").trim();
            acceptBeforeReady = boolOr(p.getProperty("host.acceptBeforeReady"), true);
            autoRoomOnLan = boolOr(p.getProperty("lan.autoRoom"), true);
            mode = RouteMode.of(p.getProperty("mode"));
            fecEnabled = boolOr(p.getProperty("fec.enabled"), true);
            fecGroup = intOr(p.getProperty("fec.group"), 10);
            fecParity = intOr(p.getProperty("fec.parity"), 2);
            Trace.setVerbose(verbose);
        } catch (Throwable t) {
            Trace.warn("[P2P] Reading the config failed, using defaults: {}", t.toString());
        }
    }

    private static void writeDefault() {
        try {
            Files.createDirectories(FILE.getParent());
            Properties p = new Properties();
            p.setProperty("relay.host", "");
            p.setProperty("relay.port", "34777");
            p.setProperty("relay.embedded", "true");
            p.setProperty("host.punchPort", "34778");
            p.setProperty("verbose", "false");
            p.setProperty("forceRelay", "false");
            p.setProperty("stun.servers", "");
            p.setProperty("stun.enabled", "true");
            p.setProperty("relay.idleTimeoutSec", "90");

            p.setProperty("relay.forward", "true");
            p.setProperty("room", "");
            p.setProperty("host.acceptBeforeReady", "true");
            p.setProperty("lan.autoRoom", "true");
            p.setProperty("mode", RouteMode.AUTO.id);
            p.setProperty("fec.enabled", "true");
            p.setProperty("fec.group", "10");
            p.setProperty("fec.parity", "2");
            try (OutputStream out = Files.newOutputStream(FILE)) {
                p.store(new java.io.OutputStreamWriter(out, java.nio.charset.StandardCharsets.UTF_8),
                        "p2pmod");
            }
        } catch (IOException ignored) {
        }
    }

    public static synchronized void saveRoom(String room) {
        String value = (room == null) ? "" : room;
        if (writeOne("room", value)) defaultRoom = value;
    }

    public static synchronized void saveRelay(String host, int port) {
        String value = (host == null) ? "" : host.trim();
        if (relayEmbedded && isLoopback(value)) value = "";
        if (writeOne("relay.host", value)) relayHost = value;
        if (writeOne("relay.port", String.valueOf(port))) relayPort = port;
    }

    public static synchronized void saveMode(RouteMode m) {
        if (m == null) return;
        mode = m;
        writeOne("mode", m.id);
    }

    public static boolean isLoopback(String host) {
        if (host == null) return false;
        String h = host.trim();
        return h.equals("127.0.0.1") || h.equals("::1") || h.equalsIgnoreCase("localhost");
    }

    private static boolean writeOne(String key, String value) {
        try {
            Properties p = new Properties();
            if (Files.isRegularFile(FILE)) {
                try (InputStream in = Files.newInputStream(FILE)) {
                    p.load(new java.io.InputStreamReader(in, java.nio.charset.StandardCharsets.UTF_8));
                }
            }
            p.setProperty(key, value);
            Files.createDirectories(FILE.getParent());
            try (OutputStream out = Files.newOutputStream(FILE)) {
                p.store(new java.io.OutputStreamWriter(out, java.nio.charset.StandardCharsets.UTF_8),
                        "p2pmod");
            }
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    private static String[] splitServers(String raw) {
        if (raw == null || raw.isBlank()) return new String[0];
        String[] parts = raw.split(",");
        java.util.List<String> out = new java.util.ArrayList<>();
        for (String one : parts) {
            String t = one.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out.toArray(new String[0]);
    }

    private static int intOr(String s, int fallback) {
        if (s == null) return fallback;
        try { return Integer.parseInt(s.trim()); } catch (NumberFormatException e) { return fallback; }
    }

    private static boolean boolOr(String s, boolean fallback) {
        return s == null ? fallback : Boolean.parseBoolean(s.trim());
    }
}
