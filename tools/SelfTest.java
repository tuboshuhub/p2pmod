import dev.p2pmod.core.Bytes;
import dev.p2pmod.core.Mux;
import dev.p2pmod.core.Path;
import dev.p2pmod.core.Receiver;
import dev.p2pmod.core.Sender;
import dev.p2pmod.core.StunProbe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Self-check for the p2pmod transport layer. Runs against the built jar with no
 * Minecraft and no network, so it can be run after every change:
 *
 *   tools/selftest.sh
 *
 * Exit code 0 means every case passed.
 */
public final class SelfTest {

    private static int passed;
    private static int failed;

    public static void main(String[] args) throws Exception {
        muxRoundTrip();
        controlCarriesCredit();
        receiverDeliversInOrder();
        receiverFillsGaps();
        receiverIgnoresDuplicates();
        receiverSurvivesCorruptLength();
        receiverAdvertisesCredit();
        queuedFramesAreNotRetransmitted();
        windowIsCappedByBandwidthDelayProduct();
        peerCreditPausesIntake();
        relayPenaltyIsProportional();
        stunParsesXorMappedAddress();

        System.out.println();
        System.out.println(failed == 0
                ? "ALL PASSED (" + passed + " checks)"
                : "FAILED " + failed + " of " + (passed + failed) + " checks");
        System.exit(failed == 0 ? 0 : 1);
    }

    // ---------------------------------------------------------------- cases

    private static void muxRoundTrip() {
        byte[] payload = "hello-minecraft".getBytes();
        byte[] frame = Mux.frame(42, payload, 0, payload.length);
        check("mux seq round-trip", Mux.seqOf(frame, 0) == 42, "seq=" + Mux.seqOf(frame, 0));
        check("mux len round-trip", Mux.lenOf(frame, 0) == payload.length, "len=" + Mux.lenOf(frame, 0));
        byte[] ctrl = Mux.control(Mux.CTRL_NACK, 7);
        check("mux ctrl seq is CTRL_SEQ", Mux.seqOf(ctrl, 0) == Mux.CTRL_SEQ, "seq=" + Mux.seqOf(ctrl, 0));
        check("mux ctrl value", Mux.ctrlValueOf(slice(ctrl)) == 7, "v=" + Mux.ctrlValueOf(slice(ctrl)));
        check("legacy ctrl has no credit", Mux.ctrlCreditOf(slice(ctrl)) == -1, "credit should be -1");
    }

    private static void controlCarriesCredit() {
        byte[] ack = Mux.controlWithCredit(Mux.CTRL_ACK, 1234, 5555);
        byte[] body = slice(ack);
        check("credit ack keeps cumulative", Mux.ctrlValueOf(body) == 1234, "v=" + Mux.ctrlValueOf(body));
        check("credit value round-trip", Mux.ctrlCreditOf(body) == 5555, "credit=" + Mux.ctrlCreditOf(body));
        check("credit ack stays a control frame", Mux.seqOf(ack, 0) == Mux.CTRL_SEQ, "seq");
        check("truncated credit is rejected", Mux.ctrlCreditOf(new byte[12]) == -1, "short body");
    }

    private static void receiverDeliversInOrder() throws Exception {
        Sink r = new Sink();
        for (int i = 0; i < 12; i++) r.receiver.ingest(Mux.frame(i, ("D" + i).getBytes(), 0, ("D" + i).length()));
        r.settle(400);
        check("in-order delivery", r.delivered.size() == 12, "got " + r.delivered.size() + "/12");
        check("in-order sequence", r.delivered.equals(List.of(
                "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "D10", "D11")),
                String.valueOf(r.delivered));
        r.receiver.close();
    }

    private static void receiverFillsGaps() throws Exception {
        Sink r = new Sink();
        int[] order = {0, 1, 3, 4, 2, 5};           // 2 arrives late
        for (int seq : order) r.receiver.ingest(Mux.frame(seq, ("G" + seq).getBytes(), 0, 2));
        r.settle(400);
        check("gap filled in order", r.delivered.equals(List.of("G0", "G1", "G2", "G3", "G4", "G5")),
                String.valueOf(r.delivered));
        r.receiver.close();
    }

    private static void receiverIgnoresDuplicates() throws Exception {
        Sink r = new Sink();
        for (int i = 0; i < 4; i++) r.receiver.ingest(Mux.frame(i, ("P" + i).getBytes(), 0, 2));
        for (int i = 0; i < 4; i++) r.receiver.ingest(Mux.frame(i, ("P" + i).getBytes(), 0, 2));   // replay
        r.settle(400);
        check("duplicates are not re-delivered", r.delivered.size() == 4, "got " + r.delivered.size());
        r.receiver.close();
    }

    private static void receiverSurvivesCorruptLength() throws Exception {
        Sink r = new Sink();
        byte[] bogus = new byte[Mux.HEADER];
        Bytes.putInt(bogus, 8, Mux.MAX_PAYLOAD + 1);        // claims an impossible length
        r.receiver.ingest(bogus);
        for (int i = 0; i < 3; i++) r.receiver.ingest(Mux.frame(i, ("R" + i).getBytes(), 0, 2));
        r.settle(400);
        check("stream realigns after a corrupt header", r.delivered.size() == 3, "got " + r.delivered.size());
        r.receiver.close();
    }

    private static void receiverAdvertisesCredit() throws Exception {
        Sink r = new Sink();
        for (int i = 0; i < 8; i++) r.receiver.ingest(Mux.frame(i, "X".getBytes(), 0, 1));
        r.settle(300);
        check("receiver reports free delivery slots", r.receiver.credit() > 0, "credit=" + r.receiver.credit());
        check("ack carried a credit value", r.lastCredit >= 0, "lastCredit=" + r.lastCredit);
        r.receiver.close();
    }

    private static void queuedFramesAreNotRetransmitted() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        Path slow = slowPath(50, sent);
        Sender s = new Sender(1, () -> slow);

        byte[] chunk = new byte[Mux.FRAG_SIZE];
        for (int i = 0; i < 200; i++) s.send(chunk);

        long t0 = System.currentTimeMillis();
        while (System.currentTimeMillis() - t0 < 1000) {
            s.tick();
            Thread.sleep(50);
        }
        int after = sent.get();
        // ~20 first transmissions fit in one second; a handful of genuine retransmits for
        // frames that were actually sent but never ACKed. The old code re-sent every
        // still-queued frame and landed well above 100.
        check("queued frames are not retransmitted", after <= 40, after + " packets in 1s");
        s.close();
    }

    private static void windowIsCappedByBandwidthDelayProduct() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        Sender s = new Sender(2, () -> slowPath(50, sent));

        setField(s, "ackedRate", 192);          // ~250 KB/s at 1300 B/frame -> BDP ~10 frames
        Field grow = Sender.class.getDeclaredField("lastCwndGrowMs");
        grow.setAccessible(true);
        Method growCwnd = Sender.class.getDeclaredMethod("growCwnd", long.class);
        growCwnd.setAccessible(true);
        for (int i = 0; i < 60; i++) {
            grow.setLong(s, 0);
            growCwnd.invoke(s, System.currentTimeMillis());
        }
        int cwnd = s.cwnd();
        check("window capped near the BDP", cwnd <= 128, "cwnd=" + cwnd + " (ceiling was 8192)");
        s.close();
    }

    private static void peerCreditPausesIntake() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        Sender s = new Sender(3, () -> slowPath(0, sent));
        s.onCredit(0);

        Thread t = new Thread(() -> {
            try {
                s.send(new byte[Mux.FRAG_SIZE]);
            } catch (IOException ignored) {
            }
        });
        t.setDaemon(true);
        t.start();
        Thread.sleep(300);
        check("credit 0 blocks new data", t.isAlive(), "sender returned while the peer reported a full buffer");

        s.onCredit(1024);
        t.join(3000);
        check("credit restores sending", !t.isAlive(), "sender still blocked after credit returned");
        s.close();
    }

    private static void relayPenaltyIsProportional() {
        Path direct = new Path(Path.Via.DIRECT, b -> { }, LOOPBACK, "direct");
        direct.promote();
        direct.noteRtt(300);
        Path relay = new Path(Path.Via.RELAY, b -> { }, LOOPBACK, "relay");
        relay.promote();
        relay.noteRtt(300);
        int delta = relay.score() - direct.score();
        check("relay loses to an equal-latency direct leg", delta > 0, "delta=" + delta);
        check("relay penalty scales with latency", delta >= 60, "delta=" + delta + " (flat penalty used to be 30)");
    }

    private static void stunParsesXorMappedAddress() throws Exception {
        byte[] txid = new byte[12];
        for (int i = 0; i < 12; i++) txid[i] = (byte) (i + 1);

        byte[] ip = {1, 2, 3, 4};
        int port = 5555;
        int magic = 0x2112A442;

        byte[] resp = new byte[20 + 4 + 8];
        resp[0] = 0x01;
        resp[1] = 0x01;
        Bytes.putInt(resp, 4, magic);
        System.arraycopy(txid, 0, resp, 8, 12);

        int off = 20;
        resp[off] = 0x00;                       // attribute type 0x0020 (XOR-MAPPED-ADDRESS)
        resp[off + 1] = 0x20;
        resp[off + 2] = 0x00;                   // length 8
        resp[off + 3] = 0x08;
        resp[off + 4] = 0x00;                   // reserved
        resp[off + 5] = 0x01;                   // IPv4
        int xport = port ^ (magic >>> 16);
        resp[off + 6] = (byte) (xport >> 8);
        resp[off + 7] = (byte) xport;
        for (int i = 0; i < 4; i++) {
            resp[off + 8 + i] = (byte) (ip[i] ^ (byte) ((magic >>> (24 - i * 8)) & 0xFF));
        }

        InetSocketAddress out = StunProbe.parse(resp, txid);
        check("stun decodes the mapped address",
                out != null && out.getPort() == port && out.getAddress().equals(InetAddress.getByAddress(ip)),
                String.valueOf(out));
        check("stun rejects a foreign transaction id",
                StunProbe.parse(resp, new byte[12]) == null, "txid mismatch should be dropped");
    }

    // -------------------------------------------------------------- helpers

    private static final InetSocketAddress LOOPBACK = new InetSocketAddress("127.0.0.1", 1234);

    private static Path slowPath(int delayMs, AtomicInteger counter) {
        Path p = new Path(Path.Via.DIRECT, pkt -> {
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            counter.incrementAndGet();
        }, LOOPBACK, "slow");
        p.promote();
        p.noteRtt(Math.max(1, delayMs));
        return p;
    }

    private static byte[] slice(byte[] frame) {
        byte[] body = new byte[frame.length - Mux.HEADER];
        System.arraycopy(frame, Mux.HEADER, body, 0, body.length);
        return body;
    }

    private static void setField(Object target, String name, int value) throws Exception {
        Field f = Sender.class.getDeclaredField(name);
        f.setAccessible(true);
        f.setInt(target, value);
    }

    private static void check(String name, boolean ok, String detail) {
        if (ok) {
            passed++;
            System.out.println("  PASS  " + name);
        } else {
            failed++;
            System.out.println("  FAIL  " + name + "  <-- " + detail);
        }
    }

    /** Receiver wired to recording sinks. */
    private static final class Sink {
        final List<String> delivered = Collections.synchronizedList(new ArrayList<String>());
        final AtomicLong lastAck = new AtomicLong(-2);
        volatile int lastCredit = -1;
        final Receiver receiver;

        Sink() {
            receiver = new Receiver(
                    new Receiver.ControlSink() {
                        public void onAck(long cumulative, int credit) {
                            lastAck.set(cumulative);
                            lastCredit = credit;
                        }
                        public void onNack(long missing) { }
                    },
                    new Receiver.Feedback() {
                        public void ack(long cumulative, int credit) { lastCredit = credit; }
                        public void nack(long missing) { }
                    },
                    data -> delivered.add(new String(data)));
        }

        void settle(long ms) throws InterruptedException {
            Thread.sleep(ms);
        }
    }
}
