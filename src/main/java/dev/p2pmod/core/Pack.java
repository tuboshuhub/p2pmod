package dev.p2pmod.core;

public final class Pack {

    private Pack() {}

    public static final byte MAGIC_0 = 'P';
    public static final byte MAGIC_1 = 'C';

    public static final int HEADER = 7;

    public static final byte KIND_DATA      = 0x01;
    public static final byte KIND_CHALLENGE = 0x02;
    public static final byte KIND_RESPONSE  = 0x03;
    public static final byte KIND_PING      = 0x04;
    public static final byte KIND_PONG      = 0x05;
    public static final byte KIND_CLOSE     = 0x06;

    public static final byte KIND_NATADDR   = 0x07;

    public static final byte KIND_MODE      = 0x08;

    public static byte[] build(int connId, byte kind, byte[] body, int off, int len) {
        byte[] out = new byte[HEADER + len];
        out[0] = MAGIC_0;
        out[1] = MAGIC_1;
        Bytes.putInt(out, 2, connId);
        out[6] = kind;
        System.arraycopy(body, off, out, HEADER, len);
        return out;
    }

    public static byte[] build(int connId, byte kind) {
        return build(connId, kind, new byte[0], 0, 0);
    }

    public static boolean isPath(byte[] buf, int len) {
        return len >= HEADER && buf[0] == MAGIC_0 && buf[1] == MAGIC_1;
    }

    public static int connIdOf(byte[] buf) {
        return Bytes.getInt(buf, 2);
    }

    public static byte kindOf(byte[] buf) {
        return buf[6];
    }

    public static byte[] bodyOf(byte[] buf, int len) {
        if (len <= HEADER) return new byte[0];
        byte[] b = new byte[len - HEADER];
        System.arraycopy(buf, HEADER, b, 0, b.length);
        return b;
    }
}
