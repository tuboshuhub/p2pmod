package dev.p2pmod.edge;

import dev.p2pmod.core.RetryableIOException;
import dev.p2pmod.support.Trace;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public final class Backend implements Closeable {

    public interface Owner {
        void upstream(byte[] data, int off, int len) throws IOException;
        void onClosed();
    }

    private final Owner owner;
    private volatile Socket socket;
    private volatile OutputStream out;
    private volatile boolean running;

    public Backend(Owner owner) {
        this.owner = owner;
    }

    public boolean connected() {
        Socket s = socket;
        return running && s != null && !s.isClosed() && s.isConnected();
    }

    public int localPort() {
        Socket s = socket;
        return s == null || s.isClosed() ? -1 : s.getLocalPort();
    }

    public void connect(String host, int port) throws IOException {

        Socket old;
        synchronized (this) {
            old = socket;
            socket = null;
            out = null;
            running = false;
        }
        close(old);

        Socket s = new Socket();
        s.connect(new InetSocketAddress(host, port), 5_000);
        s.setTcpNoDelay(true);
        synchronized (this) {
            socket = s;
            out = s.getOutputStream();
            running = true;
        }
        Thread reader = new Thread(() -> readLoop(s), "p2p-backend-read");
        reader.setDaemon(true);
        reader.start();
    }

    private void readLoop(Socket own) {
        byte[] buf = new byte[16384];
        try {
            InputStream in = own.getInputStream();
            int n;
            while ((n = in.read(buf)) != -1) {
                if (!running || socket != own) break;
                IOException last = null;
                boolean ok = false;
                long deadline = 0;
                while (!ok) {
                    try {
                        owner.upstream(buf, 0, n);
                        ok = true;
                    } catch (IOException e) {
                        last = e;

                        if (!(e instanceof RetryableIOException retryable)) break;
                        if (deadline == 0) deadline = System.currentTimeMillis() + retryable.budgetMs();
                        if (System.currentTimeMillis() >= deadline) break;
                        sleep(20);
                    }
                }
                if (!ok) {
                    Trace.warn("[P2P] Upstream of MC return data failed: {} -- closing this connection",
                            last == null ? "unknown" : last.toString());
                    break;
                }
            }
        } catch (IOException ignored) {
        } finally {
            close(own);
            boolean wasCurrent;
            synchronized (this) {

                wasCurrent = (socket == own);
                if (wasCurrent) {
                    running = false;
                    socket = null;
                    out = null;
                }
            }
            if (wasCurrent) owner.onClosed();
        }
    }

    public void deliver(byte[] data) {
        OutputStream o = out;
        if (o == null) return;
        try {
            synchronized (this) {
                if (out != null) out.write(data);
            }
        } catch (IOException e) {
            Trace.warn("[P2P] Write to local MC failed ({}B): {} -- rebuild requested",
                    data.length, e.toString());
            running = false;
            close(socket);
            owner.onClosed();
        }
    }

    @Override
    public void close() {
        running = false;
        close(socket);
    }

    private static void close(Socket s) {
        if (s != null) {
            try { s.close(); } catch (IOException ignored) {}
        }
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
