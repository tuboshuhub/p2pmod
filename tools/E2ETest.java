import dev.p2pmod.core.Connection;
import dev.p2pmod.core.Endpoint;
import dev.p2pmod.core.Signaling;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end test over REAL loopback UDP sockets.
 *
 * Unlike SelfTest (pure in-JVM logic), this exercises the whole stack:
 * Endpoint rx loop -> Pack framing -> Connection routing -> Sender ARQ/pacing/credit
 * -> relay signalling -> peer Receiver in-order delivery.
 *
 * A real UDP relay is implemented below so both peers talk through an actual
 * third socket, and packet loss is injected at that relay.
 */
public final class E2ETest {

    private static int pass = 0;
    private static int fail = 0;

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("--big")) {
            bigTransfer();
            return;
        }
        System.out.println("=== p2pmod end-to-end test (real UDP on 127.0.0.1) ===");

        try (Relay relay = new Relay()) {
            System.out.println("relay listening on " + relay.addr());

            cleanLinkTransfer(relay);
            lossyLinkTransfer(relay);
            bidirectionalTransfer(relay);
            manySmallWrites(relay);
            heavyLossDoesNotStall(relay);
        }

        System.out.println();
        if (fail == 0) {
            System.out.println("E2E ALL PASSED (" + pass + " checks, 0 failures)");
            System.exit(0);
        }
        System.out.println("E2E FAILED (" + pass + " passed, " + fail + " failed)");
        System.exit(1);
    }

    // ---------------------------------------------------------------- cases

    /**
     * Not part of the suite: a long clean transfer, used to check that the pacing
     * controller actually converges instead of sitting at its ramp-up value.
     *   java E2ETest --big
     */
    private static void bigTransfer() throws Exception {
        int mb = 16;
        try (Relay relay = new Relay()) {
            System.out.println("=== throughput run: " + mb + " MB, lossless loopback ===");
            byte[] data = randomBytes(mb * 1024 * 1024, 9);
            Result r = transfer(relay, data, data, 300_000);
            System.out.printf("  %d bytes in %d ms -> %.0f KB/s, exact=%s, relay fwd=%d%n",
                    data.length, r.ms, data.length / 1024.0 / (r.ms / 1000.0),
                    Arrays.equals(r.aToB, data), relay.forwarded.get());
        }
    }

    /** 2 MB through a lossless link must arrive byte-for-byte. */
    private static void cleanLinkTransfer(Relay relay) throws Exception {
        relay.dropRate = 0;
        byte[] data = randomBytes(2 * 1024 * 1024, 1);
        Result r = transfer(relay, data, data, 60_000);
        check("clean 2MB delivered complete", r.aToB.length == data.length,
                r.aToB.length + "/" + data.length + " bytes");
        check("clean 2MB is byte-exact", Arrays.equals(r.aToB, data), "sha=" + sha(r.aToB).substring(0, 12));
        System.out.printf("       -> %d bytes in %d ms (%.0f KB/s), relay fwd=%d drop=%d%n",
                data.length, r.ms, data.length / 1024.0 / (r.ms / 1000.0), relay.forwarded.get(), relay.dropped.get());
    }

    /** 10% injected packet loss: ARQ must still deliver the exact stream. */
    private static void lossyLinkTransfer(Relay relay) throws Exception {
        relay.dropRate = 0.10;
        byte[] data = randomBytes(2 * 1024 * 1024, 2);
        Result r = transfer(relay, data, data, 120_000);
        check("10% loss: delivered complete", r.aToB.length == data.length,
                r.aToB.length + "/" + data.length + " bytes");
        check("10% loss: byte-exact after retransmission", Arrays.equals(r.aToB, data),
                "sha=" + sha(r.aToB).substring(0, 12));
        System.out.printf("       -> %d bytes in %d ms; relay dropped %d packets%n",
                data.length, r.ms, relay.dropped.get());
        relay.dropRate = 0;
    }

    /** Both directions at once, each must stay intact (no cross-talk in the mux). */
    private static void bidirectionalTransfer(Relay relay) throws Exception {
        relay.dropRate = 0.02;
        byte[] up = randomBytes(512 * 1024, 3);
        byte[] down = randomBytes(512 * 1024, 4);
        Result r = transfer(relay, up, down, 90_000);
        check("bidir: A->B complete", r.aToB.length == up.length, r.aToB.length + "/" + up.length);
        check("bidir: B->A complete", r.bToA.length == down.length, r.bToA.length + "/" + down.length);
        check("bidir: A->B exact", Arrays.equals(r.aToB, up), "");
        check("bidir: B->A exact", Arrays.equals(r.bToA, down), "");
        relay.dropRate = 0;
    }

    /** Many tiny writes must not be coalesced away or reordered. */
    private static void manySmallWrites(Relay relay) throws Exception {
        relay.dropRate = 0.05;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        Random rnd = new Random(7);
        for (int i = 0; i < 2000; i++) {
            byte[] chunk = new byte[1 + rnd.nextInt(40)];
            for (int j = 0; j < chunk.length; j++) chunk[j] = (byte) (i * 31 + j);
            buf.write(chunk);
        }
        byte[] payload = buf.toByteArray();
        Result r = transfer(relay, payload, payload, 90_000);
        check("2000 tiny writes: complete", r.aToB.length == payload.length,
                r.aToB.length + "/" + payload.length);
        check("2000 tiny writes: byte-exact (order preserved)", Arrays.equals(r.aToB, payload), "");
        relay.dropRate = 0;
    }

    /**
     * 30% loss is the shape of the historical stall (recv frozen, retransmissions
     * forever). The link must still deliver, or at worst give up cleanly -- never hang.
     */
    private static void heavyLossDoesNotStall(Relay relay) throws Exception {
        relay.dropRate = 0.30;
        byte[] data = randomBytes(256 * 1024, 5);
        long t0 = System.currentTimeMillis();
        Result r = transfer(relay, data, data, 120_000);
        long ms = System.currentTimeMillis() - t0;
        check("30% loss: did not hang (finished in <120s)", ms < 120_000, ms + "ms");
        check("30% loss: byte-exact", Arrays.equals(r.aToB, data),
                r.aToB.length + "/" + data.length + " bytes");
        System.out.printf("       -> %d bytes in %d ms; relay dropped %d packets%n",
                data.length, ms, relay.dropped.get());
        relay.dropRate = 0;
    }

    // ---------------------------------------------------------------- harness

    private record Result(byte[] aToB, byte[] bToA, long ms) {}

    private static Result transfer(Relay relay, byte[] aToB, byte[] bToA, long timeoutMs) throws Exception {
        int connId = 7000 + (int) (System.currentTimeMillis() % 1000);
        try (Peer a = new Peer(connId, relay.addr(), 1001, 2002);
             Peer b = new Peer(connId, relay.addr(), 2002, 1001)) {

            awaitUsable(a, b);

            long t0 = System.currentTimeMillis();
            Thread t1 = writeAsync(a, aToB);
            Thread t2 = writeAsync(b, bToA);
            t1.join();
            t2.join();

            byte[] receivedByB = b.collect(aToB.length, timeoutMs);
            byte[] receivedByA = a.collect(bToA.length, timeoutMs);
            long ms = System.currentTimeMillis() - t0;
            return new Result(receivedByB, receivedByA, ms);
        }
    }

    private static void awaitUsable(Peer a, Peer b) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (a.conn.preferred() != null && b.conn.preferred() != null) return;
            Thread.sleep(10);
        }
        throw new IllegalStateException("link never became usable (no preferred path)");
    }

    private static Thread writeAsync(Peer p, byte[] data) {
        Thread t = new Thread(() -> {
            try {
                int off = 0;
                while (off < data.length) {
                    int n = Math.min(9000, data.length - off);
                    p.conn.send(data, off, n);
                    off += n;
                }
            } catch (Exception e) {
                System.out.println("       !! write failed: " + e);
            }
        }, "e2e-writer");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static final class Peer implements Closeable {
        final Endpoint ep;
        final Connection conn;
        private final Queue<byte[]> chunks = new ConcurrentLinkedQueue<>();
        private final AtomicLong rxBytes = new AtomicLong();

        Peer(int connId, InetSocketAddress relay, int myToken, int peerToken) throws SocketException {
            ep = new Endpoint(0);
            ep.start();
            conn = new Connection(connId, ep, data -> {
                chunks.add(data);
                rxBytes.addAndGet(data.length);
            });
            conn.setAllowDirect(false);
            conn.addRelay(relay, myToken, peerToken);
        }

        byte[] collect(int expected, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline && rxBytes.get() < expected) {
                Thread.sleep(10);
            }
            conn.drain(3_000);
            Thread.sleep(100);
            ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(expected, 16));
            for (byte[] c : chunks) out.write(c, 0, c.length);
            return out.toByteArray();
        }

        @Override
        public void close() {
            try { conn.close(); } catch (Throwable ignored) { }
            ep.close();
        }
    }

    /** A real UDP relay socket; speaks OP_RELAY and forwards the inner packet. */
    private static final class Relay implements Closeable {
        final DatagramSocket sock;
        final InetSocketAddress addr;
        final Map<Integer, InetSocketAddress> tokens = new ConcurrentHashMap<>();
        final AtomicLong forwarded = new AtomicLong();
        final AtomicLong dropped = new AtomicLong();
        volatile double dropRate;
        private volatile boolean running = true;
        private final Random rnd = new Random(12345);

        Relay() throws SocketException {
            sock = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
            addr = new InetSocketAddress("127.0.0.1", sock.getLocalPort());
            Thread t = new Thread(this::loop, "e2e-relay");
            t.setDaemon(true);
            t.start();
        }

        InetSocketAddress addr() { return addr; }

        private void loop() {
            byte[] buf = new byte[65535];
            DatagramPacket p = new DatagramPacket(buf, buf.length);
            while (running) {
                try {
                    sock.receive(p);
                } catch (Exception e) {
                    if (running) continue;
                    return;
                }
                int len = p.getLength();
                byte[] raw = Arrays.copyOf(p.getData(), len);
                InetSocketAddress from = new InetSocketAddress(p.getAddress(), p.getPort());
                if (!Signaling.isSignaling(raw, len) || Signaling.opOf(raw) != Signaling.OP_RELAY) continue;

                tokens.put(Signaling.tokenOf(raw), from);
                InetSocketAddress dest = tokens.get(Signaling.toTokenOf(raw));
                if (dest == null) continue;

                if (dropRate > 0 && rnd.nextDouble() < dropRate) {
                    dropped.incrementAndGet();
                    continue;
                }
                byte[] inner = Signaling.forwardBodyOf(raw, len);
                try {
                    sock.send(new DatagramPacket(inner, inner.length, dest));
                    forwarded.incrementAndGet();
                } catch (Exception ignored) {
                }
            }
        }

        @Override
        public void close() {
            running = false;
            sock.close();
        }
    }

    // ---------------------------------------------------------------- utils

    private static byte[] randomBytes(int n, long seed) {
        byte[] b = new byte[n];
        new Random(seed).nextBytes(b);
        return b;
    }

    private static String sha(byte[] b) {
        try {
            StringBuilder sb = new StringBuilder();
            for (byte x : MessageDigest.getInstance("SHA-256").digest(b)) {
                sb.append(String.format("%02x", x));
            }
            return sb.toString();
        } catch (Exception e) {
            return "?";
        }
    }

    private static void check(String name, boolean ok, String detail) {
        if (ok) {
            pass++;
            System.out.println("  PASS  " + name);
        } else {
            fail++;
            System.out.println("  FAIL  " + name + (detail.isEmpty() ? "" : "  [" + detail + "]"));
        }
    }
}
