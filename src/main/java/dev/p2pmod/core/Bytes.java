package dev.p2pmod.core;

public final class Bytes {

    private Bytes() {}

    public static void putInt(byte[] buf, int off, int v) {
        buf[off]     = (byte) (v >>> 24);
        buf[off + 1] = (byte) (v >>> 16);
        buf[off + 2] = (byte) (v >>> 8);
        buf[off + 3] = (byte) v;
    }

    public static int getInt(byte[] buf, int off) {
        return ((buf[off] & 0xFF) << 24) | ((buf[off + 1] & 0xFF) << 16)
                | ((buf[off + 2] & 0xFF) << 8) | (buf[off + 3] & 0xFF);
    }

    public static void putLong(byte[] buf, int off, long v) {
        for (int i = 7; i >= 0; i--) { buf[off + i] = (byte) v; v >>>= 8; }
    }

    public static long getLong(byte[] buf, int off) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (buf[off + i] & 0xFF);
        return v;
    }

    public static final int ADDR_LEN = 19;

    private static final int FAMILY_V4 = 4;
    private static final int FAMILY_V6 = 6;

    public static void putAddr(byte[] buf, int off, java.net.InetSocketAddress addr) {
        java.util.Arrays.fill(buf, off, off + ADDR_LEN, (byte) 0);
        java.net.InetAddress a = normalize(addr);
        if (a == null) return;
        byte[] octets = a.getAddress();
        buf[off] = (byte) (octets.length == 16 ? FAMILY_V6 : FAMILY_V4);
        System.arraycopy(octets, 0, buf, off + 1, octets.length);
        int p = addr.getPort();
        buf[off + 17] = (byte) (p >>> 8);
        buf[off + 18] = (byte) p;
    }

    public static java.net.InetSocketAddress getAddr(byte[] buf, int off) {
        if (buf.length < off + ADDR_LEN) return null;
        int family = buf[off] & 0xFF;
        int octets = family == FAMILY_V6 ? 16 : family == FAMILY_V4 ? 4 : 0;
        if (octets == 0) return null;
        byte[] ip = new byte[octets];
        System.arraycopy(buf, off + 1, ip, 0, octets);
        int port = ((buf[off + 17] & 0xFF) << 8) | (buf[off + 18] & 0xFF);
        try {
            return new java.net.InetSocketAddress(
                    normalize(java.net.InetAddress.getByAddress(ip)), port);
        } catch (Exception e) {
            return null;
        }
    }

    public static java.net.InetAddress normalize(java.net.InetSocketAddress addr) {
        if (addr == null) return null;
        java.net.InetAddress a = addr.getAddress();
        if (a == null) {
            try {
                a = java.net.InetAddress.getByName(addr.getHostString());
            } catch (Exception e) {
                return null;
            }
        }
        return normalize(a);
    }

    public static boolean isPrivate(java.net.InetAddress a) {
        a = normalize(a);
        if (a == null) return true;
        if (a.isLoopbackAddress() || a.isAnyLocalAddress() || a.isLinkLocalAddress()
                || a.isSiteLocalAddress() || a.isMulticastAddress()) {
            return true;
        }
        byte[] b = a.getAddress();
        if (b.length == 16) {
            int first = b[0] & 0xFF;
            return first == 0xFC || first == 0xFD;
        }
        return false;
    }

    public static java.net.InetAddress normalize(java.net.InetAddress a) {
        if (a == null) return null;
        byte[] b = a.getAddress();
        if (b.length != 16) return a;
        for (int i = 0; i < 10; i++) if (b[i] != 0) return a;
        if (b[10] != (byte) 0xFF || b[11] != (byte) 0xFF) return a;
        byte[] v4 = {b[12], b[13], b[14], b[15]};
        try {
            return java.net.InetAddress.getByAddress(v4);
        } catch (Exception e) {
            return a;
        }
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    public static String hex(byte[] b) {
        char[] c = new char[b.length * 2];
        for (int i = 0; i < b.length; i++) {
            c[i * 2] = HEX[(b[i] >>> 4) & 0xF];
            c[i * 2 + 1] = HEX[b[i] & 0xF];
        }
        return new String(c);
    }
}
