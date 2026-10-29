package dev.p2pmod.relay;

import dev.p2pmod.core.Signaling;
import dev.p2pmod.core.StunProbe;
import dev.p2pmod.support.Config;
import dev.p2pmod.support.Trace;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public final class RelayMain {

    private static final String VERSION = "1.0.0";

    private static final int DEFAULT_PORT = 34777;
    private static final CountDownLatch STOP = new CountDownLatch(1);
    private static final java.security.SecureRandom RNG = new java.security.SecureRandom();

    private RelayMain() {}

    public static void main(String[] args) {
        mirrorToFile();
        int portOverride = -1;
        int statsSec = 60;
        boolean selftest = false;
        String probeTarget = null;

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "--port", "-p" -> {
                    if (i + 1 >= args.length) die("--port needs a port number", 2);
                    portOverride = parseInt(args[++i], "--port");
                }
                case "--stats" -> {
                    if (i + 1 >= args.length) die("--stats needs an interval in seconds (0 = off)", 2);
                    statsSec = parseInt(args[++i], "--stats");
                }
                case "--selftest" -> selftest = true;
                case "--probe" -> {
                    if (i + 1 >= args.length) die("--probe needs host or host:port", 2);
                    probeTarget = args[++i];
                }
                case "--help", "-h" -> {
                    usage();
                    return;
                }
                case "--version", "-v" -> {
                    System.out.println("p2pmod relay " + VERSION);
                    return;
                }
                default -> die("Unknown argument: " + a + " (see --help)", 2);
            }
        }

        Config.load();

        if (probeTarget != null) {
            System.exit(probeRemote(probeTarget) ? 0 : 1);
            return;
        }

        final int port = portOverride > 0 ? portOverride : Config.relayPort;
        if (port <= 0 || port > 65535) die("Invalid port: " + port, 2);

        RelayService svc = new RelayService();
        try {
            svc.start(port);
        } catch (Exception e) {

            System.err.println("[P2P][relay] Cannot listen on UDP :" + port + " -- " + e);
            System.err.println("  - Port in use: pick another --port or stop the process holding it");
            System.err.println("  - Insufficient privileges: ports below 1024 need root or CAP_NET_BIND_SERVICE on Linux");
            System.exit(2);
            return;
        }

        banner(svc.port());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[P2P][relay] Stopping...");
            svc.stop();
            STOP.countDown();
        }, "p2p-relay-stop"));

        if (statsSec > 0) startStatsLoop(svc, statsSec);

        if (selftest) {
            boolean ok = loopbackProbe(svc.port());
            System.out.println(ok ? "[P2P][relay] Self-test passed: the local registration packet was answered"
                    : "[P2P][relay] Self-test failed: no answer even locally, check the port and firewall");

            svc.stop();
            System.exit(ok ? 0 : 1);
            return;
        }

        System.out.println("[P2P][relay] Ready, waiting for a server to register a room (Ctrl+C to stop)");
        try {
            STOP.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void startStatsLoop(RelayService svc, int everySec) {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(everySec * 1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    Trace.info("[P2P][relay] {}", svc.stats());
                } catch (Throwable ignored) {

                }
            }
        }, "p2p-relay-stats");
        t.setDaemon(true);
        t.start();
    }

    private static boolean loopbackProbe(int port) {
        try {
            return probe(InetAddress.getByName("127.0.0.1"), port, true);
        } catch (Exception e) {
            System.err.println("[P2P][relay] Self-test error: " + e);
            return false;
        }
    }

    private static boolean probeRemote(String target) {
        String host = target;

        int port = DEFAULT_PORT;
        dev.p2pmod.core.HostPort.Split split = dev.p2pmod.core.HostPort.split(target, DEFAULT_PORT);
        host = split.host();
        if (split.explicit()) port = split.port();
        System.out.println("[P2P][relay] Probing " + host + ":" + port + " ...");
        try {
            return probe(InetAddress.getByName(host), port, false);
        } catch (Exception e) {
            System.err.println("[P2P][relay] Address resolution failed: " + e);
            return false;
        }
    }

    private static boolean probe(InetAddress target, int port, boolean local) {
        int token = (int) (System.nanoTime() & 0x7FFFFFFF);
        byte[] req = Signaling.control(Signaling.OP_REGISTER_CLIENT, token,
                Signaling.registerBody("SelfTest", token ^ 0x5A5A5A5A));
        try (DatagramSocket sock = new DatagramSocket()) {
            sock.setSoTimeout(4_000);
            sock.send(new DatagramPacket(req, req.length, target, port));
            long t0 = System.nanoTime();
            byte[] buf = new byte[1024];
            DatagramPacket in = new DatagramPacket(buf, buf.length);
            sock.receive(in);
            int rtt = (int) ((System.nanoTime() - t0) / 1_000_000);
            int op = Signaling.opOf(in.getData());
            String body = Signaling.textBodyValue(
                    Signaling.controlBodyOf(in.getData(), in.getLength()));

            System.out.println("[P2P][relay] Answered rtt=" + rtt + "ms op=" + Signaling.opName(op)
                    + (body.isBlank() ? "" : " body=" + body));
            if (!local && op == Signaling.OP_ERROR && body.contains("room not found")) {
                System.out.println("[P2P][relay] Port reachable, but no server is currently online for that room");
            }
            return true;
        } catch (java.net.SocketTimeoutException e) {
            System.err.println("[P2P][relay] No response in 4s -- UDP " + port
                    + " may be blocked by a firewall, or no relay is running there");
            return false;
        } catch (Exception e) {
            System.err.println("[P2P][relay] Probe failed: " + e);
            return false;
        }
    }

    private static void mirrorToFile() {
        final java.io.PrintStream console = System.out;
        try {
            final java.io.FileOutputStream file = new java.io.FileOutputStream("relay.log", true);
            java.io.OutputStream tee = new java.io.OutputStream() {
                @Override public void write(int b) throws java.io.IOException {
                    console.write(b);
                    file.write(b);
                }

                @Override public void write(byte[] b, int off, int len) throws java.io.IOException {
                    console.write(b, off, len);
                    file.write(b, off, len);
                }

                @Override public void flush() throws java.io.IOException {
                    console.flush();
                    file.flush();
                }
            };
            java.io.PrintStream ps = new java.io.PrintStream(tee, true, console.charset());
            System.setOut(ps);
            System.setErr(ps);
            ps.println();
            ps.println("=== p2pmod relay starting " + java.time.LocalDateTime.now().withNano(0) + " ===");
        } catch (Exception ignored) {

        }
    }

    private static void banner(int port) {
        String ip = detectPublicIp();
        String local = localAddresses();

        String shown = (ip == null) ? "<your public address>" : ip;

        System.out.println();
        System.out.println("========================================================");
        System.out.println(" p2pmod standalone relay  v" + VERSION);
        System.out.println("--------------------------------------------------------");
        System.out.println(" Listening on     UDP " + port
                + (dev.p2pmod.core.Endpoint.dualStackSupported() ? " (IPv4 + IPv6)" : " (IPv4 only)"));
        if (ip != null) {
            System.out.println(" Public address   " + ip);
        } else {
            System.out.println(" Public address   not detected (STUN unreachable)");
            System.out.println("                  - just fill in the address you know peers can reach");
            System.out.println("                  - on a home connection, check the router's WAN address");
        }
        System.out.println(" Local addresses  " + local);
        System.out.println(" Offline timeout  " + Config.relayIdleTimeoutSec + "s");
        System.out.println(" Config file      " + Config.FILE.toAbsolutePath()
                + (java.nio.file.Files.isRegularFile(Config.FILE) ? "" : " (not created yet, defaults will be used)"));
        System.out.println("--------------------------------------------------------");
        System.out.println(" Server registers  /p2p host start " + shown + ":" + port + " <4-char room id>");
        System.out.println(" Clients connect   p2p:<room id>@" + shown);
        System.out.println("========================================================");
        System.out.println();
    }

    private static String detectPublicIp() {
        long deadline = System.currentTimeMillis() + 4_000;
        try (DatagramSocket sock = new DatagramSocket()) {
            sock.setSoTimeout(1_200);
            for (String server : StunProbe.defaultServers()) {
                if (System.currentTimeMillis() >= deadline) break;
                dev.p2pmod.core.HostPort.Split split = dev.p2pmod.core.HostPort.split(server, 3478);
                if (split.host().isEmpty()) continue;
                try {
                    InetAddress addr = InetAddress.getByName(split.host());
                    int port = split.port();
                    byte[] txid = new byte[12];
                    RNG.nextBytes(txid);
                    byte[] req = StunProbe.request(txid);
                    sock.send(new DatagramPacket(req, req.length, addr, port));

                    byte[] buf = new byte[512];
                    DatagramPacket in = new DatagramPacket(buf, buf.length);
                    sock.receive(in);
                    InetSocketAddress mapped = StunProbe.parse(
                            java.util.Arrays.copyOf(in.getData(), in.getLength()), txid);
                    if (mapped != null && mapped.getAddress() != null) {
                        return mapped.getAddress().getHostAddress();
                    }
                } catch (Exception ignored) {

                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private static String localAddresses() {
        List<String> out = new ArrayList<>();
        try {
            for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!ni.isUp() || ni.isLoopback()) continue;
                for (InetAddress a : Collections.list(ni.getInetAddresses())) {
                    if (!a.isLoopbackAddress() && !a.isLinkLocalAddress()) out.add(a.getHostAddress());
                }
            }
        } catch (Exception ignored) {
        }
        if (out.isEmpty()) return "<no network interface detected>";

        return String.join(", ", out);
    }

    private static int parseInt(String s, String what) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            die(what + " is not a number: " + s, 2);
            return -1;
        }
    }

    private static void die(String msg, int code) {
        System.err.println("[P2P][relay] " + msg);
        System.exit(code);
    }

    private static void usage() {
        System.out.println("""
                p2pmod standalone relay v%s

                Usage:
                  java -cp "p2pmod-1.0.0.jar:lib/*" dev.p2pmod.relay.RelayMain [options]

                Options:
                  --port, -p <port>   Port to listen on (defaults to relay.port, i.e. 34777)
                  --stats <seconds>   Status summary interval, 0 disables it (default 60)
                  --selftest          Run a local self-test after starting; exit code 0 on pass, 1 on failure
                  --probe <host[:port]>  Do not start a relay, only check whether that UDP port is reachable (default 34777)
                  --version, -v       Print the version
                  --help, -h          This help

                Notes:
                  This process only forwards UDP; it parses and stores no game data.
                  Room ids and timeouts are controlled by relay.idleTimeoutSec in
                  config/p2pmod.properties (default 90 seconds).
                """.formatted(VERSION));
    }
}
