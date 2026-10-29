package dev.p2pmod.core;

import dev.p2pmod.support.Trace;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class Endpoint implements Closeable {

    public interface RelayObserver {
        void onRelay(int op, int fromToken, int toToken, byte[] body, InetSocketAddress from);
    }

    public interface ConnObserver {
        void onPacket(int connId, byte kind, byte[] body, InetSocketAddress from);

        default boolean acceptsRelayFrom(int fromToken) {
            return true;
        }
    }

    private final DatagramSocket socket;
    private boolean dualStack;
    private volatile Thread rxThread;
    private volatile boolean running;

    private volatile RelayObserver relayObserver;
    private final Map<Integer, ConnObserver> conns = new ConcurrentHashMap<>();
    private final Map<String, Consumer<byte[]>> stunWaiters = new ConcurrentHashMap<>();

    private static final int BUF_BYTES = 1 * 1024 * 1024;

    public static final int PROBE_BUF_BYTES = 256 * 1024;

    public Endpoint(int localPort) throws SocketException {
        this(localPort, BUF_BYTES);
    }

    public Endpoint(int localPort, int bufBytes) throws SocketException {

        this.socket = new DatagramSocket(null);
        try {
            socket.setReuseAddress(true);
            socket.setReceiveBufferSize(bufBytes);
            socket.setSendBufferSize(bufBytes);
        } catch (SocketException e) {
            Trace.warn("[P2P] Could not enlarge the UDP buffer ({}) -- expect packet loss under load", e.getMessage());
        }
        this.dualStack = bindAny(localPort);
        Trace.debug("[P2P] UDP buffer actual recv={}B send={}B (wanted {}B), {}",
                socket.getReceiveBufferSize(), socket.getSendBufferSize(), bufBytes,
                dualStack ? "dual-stack (IPv4 + IPv6)" : "IPv4 only");
    }

    private boolean bindAny(int localPort) throws SocketException {
        try {
            socket.bind(new InetSocketAddress(java.net.InetAddress.getByName("::"), localPort));
            return true;
        } catch (Exception v6) {

            socket.bind(new InetSocketAddress(localPort));
            return false;
        }
    }

    public boolean isDualStack() {
        return dualStack;
    }

    public static boolean dualStackSupported() {
        try (DatagramSocket probe = new DatagramSocket(null)) {
            probe.bind(new InetSocketAddress(java.net.InetAddress.getByName("::"), 0));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public int localPort() {
        return socket.getLocalPort();
    }

    public void setRelayObserver(RelayObserver o) {
        this.relayObserver = o;
    }

    public void bind(int connId, ConnObserver o) {
        conns.put(connId, o);
    }

    public void unbind(int connId) {
        conns.remove(connId);
    }

    public void registerStun(byte[] transactionId, Consumer<byte[]> cb) {
        stunWaiters.put(Bytes.hex(transactionId), cb);
    }

    public void unregisterStun(byte[] transactionId) {
        stunWaiters.remove(Bytes.hex(transactionId));
    }

    public void start() {
        running = true;
        rxThread = new Thread(this::rxLoop, "p2p-udp-rx");
        rxThread.setDaemon(true);
        rxThread.start();
    }

    public void send(InetSocketAddress to, byte[] data) throws IOException {
        socket.send(new DatagramPacket(data, data.length, to));
    }

    @Override
    public void close() {
        running = false;
        socket.close();
        conns.clear();
        stunWaiters.clear();
    }

    private void rxLoop() {
        byte[] buf = new byte[2048];
        DatagramPacket p = new DatagramPacket(buf, buf.length);
        while (running) {
            try {
                socket.receive(p);
            } catch (IOException e) {
                if (running) sleepQuietly(50);
                continue;
            }
            int len = p.getLength();
            byte[] raw = p.getData();

            InetSocketAddress from = new InetSocketAddress(Bytes.normalize(p.getAddress()), p.getPort());
            try {
                dispatch(raw, len, from);
            } catch (Throwable ignored) {

            }
        }
    }

    private void dispatch(byte[] raw, int len, InetSocketAddress from) {
        if (isStun(raw, len)) {
            Consumer<byte[]> cb = stunWaiters.get(Bytes.hex(Arrays.copyOfRange(raw, 8, 20)));
            if (cb != null) cb.accept(Arrays.copyOf(raw, len));
            return;
        }
        if (Signaling.isSignaling(raw, len)) {
            int op = Signaling.opOf(raw);
            if (op == Signaling.OP_RELAY) {

                byte[] inner = Signaling.forwardBodyOf(raw, len);
                if (tryDeliver(inner, inner.length, from, Signaling.tokenOf(raw))) return;
                RelayObserver ro = relayObserver;
                if (ro != null) {
                    ro.onRelay(op, Signaling.tokenOf(raw), Signaling.toTokenOf(raw), inner, from);
                }
                return;
            }
            RelayObserver ro = relayObserver;
            if (ro != null) {
                ro.onRelay(op, Signaling.tokenOf(raw), 0, Signaling.controlBodyOf(raw, len), from);
            }
            return;
        }
        tryDeliver(raw, len, from);
    }

    private boolean tryDeliver(byte[] packet, int len, InetSocketAddress from) {
        return tryDeliver(packet, len, from, -1);
    }

    private boolean tryDeliver(byte[] packet, int len, InetSocketAddress from, int relayFromToken) {
        if (!Pack.isPath(packet, len)) return false;
        int connId = Pack.connIdOf(packet);

        ConnObserver o = conns.get(connId);
        if (o == null) return false;
        if (relayFromToken >= 0 && !o.acceptsRelayFrom(relayFromToken)) {

            Trace.periodic("[P2P] Dropped relay-forwarded frame for conn={} from unexpected token={}",
                    connId, relayFromToken);
            return true;
        }
        o.onPacket(connId, Pack.kindOf(packet), Pack.bodyOf(packet, len), from);
        return true;
    }

    private static boolean isStun(byte[] d, int len) {
        return len >= 20
                && (d[0] & 0xFF) == 0x01 && (d[1] & 0xFF) == 0x01
                && (d[4] & 0xFF) == 0x21 && (d[5] & 0xFF) == 0x12
                && (d[6] & 0xFF) == 0xA4 && (d[7] & 0xFF) == 0x42;
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
