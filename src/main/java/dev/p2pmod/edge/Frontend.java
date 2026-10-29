package dev.p2pmod.edge;

import dev.p2pmod.core.RetryableIOException;
import dev.p2pmod.support.Trace;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public final class Frontend implements Closeable {

    public interface Owner {
        void upstream(byte[] data, int off, int len) throws IOException;
        void onDisconnected(String reason);

        default void onAccepted() {}
    }

    private final Owner owner;
    private ServerSocket server;
    private volatile Socket mc;
    private volatile OutputStream mcOut;
    private volatile boolean running;

    private volatile long upBytes, downBytes;
    private volatile long lastUpMs, lastDownMs;
    private volatile boolean connected;
    private volatile long acceptedMs;
    private volatile boolean announcedUp;

    public Frontend(Owner owner) {
        this.owner = owner;
    }

    public int start() throws IOException {
        return start(0);
    }

    public int start(int preferredPort) throws IOException {
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        server = new ServerSocket();
        server.setReuseAddress(true);
        boolean fixed = false;
        if (preferredPort > 0) {
            try {
                server.bind(new InetSocketAddress(loopback, preferredPort));
                fixed = true;
            } catch (IOException e) {
                Trace.debug("[P2P] Fixed port {} unavailable ({}), using a random port", preferredPort, e.getMessage());
                try { server.close(); } catch (IOException ignored) {}
                server = new ServerSocket();
                server.setReuseAddress(true);
            }
        }
        if (!fixed) server.bind(new InetSocketAddress(loopback, 0));

        running = true;
        Thread accept = new Thread(this::acceptLoop, "p2p-frontend-accept");
        accept.setDaemon(true);
        accept.start();
        return server.getLocalPort();
    }

    public int localPort() { return server == null ? -1 : server.getLocalPort(); }

    public boolean connected() { return connected; }
    public long upBytes() { return upBytes; }
    public long downBytes() { return downBytes; }
    public long lastUpMs() { return lastUpMs; }

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = server.accept();
                s.setTcpNoDelay(true);
                s.setKeepAlive(true);
                synchronized (this) {
                    closeMc();
                    mc = s;
                    mcOut = s.getOutputStream();
                }
                connected = true;
                acceptedMs = System.currentTimeMillis();
                announcedUp = false;
                Trace.info("[P2P] Proxy :{} accepted an MC connection", server.getLocalPort());
                try {
                    owner.onAccepted();
                } catch (Throwable t) {
                    Trace.debug("[P2P] Session reset failed: {}", t.toString());
                }
                Thread reader = new Thread(() -> readLoop(s), "p2p-frontend-read");
                reader.setDaemon(true);
                reader.start();
            } catch (IOException e) {
                if (running) sleep(50);
            }
        }
    }

    private void readLoop(Socket s) {
        byte[] buf = new byte[16384];
        try {
            InputStream in = s.getInputStream();
            int n;
            while (running && (n = in.read(buf)) != -1) {
                upBytes += n;
                lastUpMs = System.currentTimeMillis();
                if (!announcedUp) {
                    announcedUp = true;
                    Trace.info("[P2P] Proxy received MC's first byte (+{}ms)",
                            System.currentTimeMillis() - acceptedMs);
                }
                push(buf, n);
            }
        } catch (IOException ignored) {
        } finally {
            Trace.info("[P2P] MC connection closed: up {}B / down {}B", upBytes, downBytes);
            connected = false;
            boolean current;
            synchronized (this) {
                current = (mc == s);
                if (current) closeMc();
            }

            if (current) owner.onDisconnected("MC disconnected");
        }
    }

    private void push(byte[] buf, int n) throws IOException {

        long deadline = 0;
        IOException last = null;
        int attempt = 0;
        while (true) {
            try {
                owner.upstream(buf, 0, n);
                return;
            } catch (IOException e) {
                last = e;
                attempt++;
                if (!(e instanceof RetryableIOException retryable)) break;
                if (deadline == 0) deadline = System.currentTimeMillis() + retryable.budgetMs();
                if (System.currentTimeMillis() >= deadline) break;
                sleep(25);
            }
        }

        if (downBytes == 0) {
            Trace.info("[P2P] Tunnel never came up after {} attempts, closing this MC connection: {}",
                    attempt, last == null ? "unknown" : last.toString());
        } else {
            Trace.warn("[P2P] Upstream failed ({} attempts, {}B already delivered); disconnecting MC instead of dropping or resending protocol data: {}",
                    attempt, downBytes, last == null ? "unknown" : last.toString());
        }
        owner.onDisconnected("upstream failed");
        throw last == null ? new IOException("upstream failed") : last;
    }

    public void deliver(byte[] data) {
        OutputStream o = mcOut;
        if (o == null) return;
        try {
            synchronized (this) {
                if (mcOut != null) mcOut.write(data);
            }
            downBytes += data.length;
            lastDownMs = System.currentTimeMillis();
        } catch (IOException e) {
            Trace.warn("[P2P] Write to MC failed ({}B): {} -- connection unusable, notifying the layer above",
                    data.length, e.toString());
            owner.onDisconnected("write to MC failed");
        }
    }

    public void kickToMc(String message) {
        byte[] pkt = disconnectPacket(message);
        synchronized (this) {
            if (mcOut != null && pkt != null) {
                try { mcOut.write(pkt); } catch (IOException ignored) {}
            }
            closeMc();
        }
        connected = false;
    }

    private synchronized void closeMc() {
        if (mc != null) {
            try { mc.close(); } catch (IOException ignored) {}
            mc = null;
            mcOut = null;
        }
    }

    @Override
    public void close() {
        running = false;
        closeMc();
        if (server != null) {
            try { server.close(); } catch (IOException ignored) {}
        }
    }

    private static byte[] disconnectPacket(String message) {
        try {
            byte[] json = ("{\"text\":\"" + escape(message) + "\"}")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] pkt = new byte[1 + varintSize(json.length) + json.length];
            int i = 0;
            pkt[i++] = 0x00;
            int v = json.length;
            while ((v & ~0x7F) != 0) {
                pkt[i++] = (byte) ((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            pkt[i++] = (byte) v;
            System.arraycopy(json, 0, pkt, i, json.length);
            return pkt;
        } catch (Throwable t) {
            return null;
        }
    }

    private static int varintSize(int v) {
        int n = 1;
        while ((v & ~0x7F) != 0) { v >>>= 7; n++; }
        return n;
    }

    private static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> sb.append(c);
            }
        }
        return sb.toString();
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
