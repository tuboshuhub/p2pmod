package dev.p2pmod.core;

public final class Mux {

    private Mux() {}

    public static final long CTRL_SEQ = -1L;

    public static final byte CTRL_ACK  = 0;
    public static final byte CTRL_NACK = 1;

    public static final int HEADER = 12;

    public static final int FRAG_SIZE = 1300;

    public static final int MAX_PAYLOAD = 1 << 20;

    public static byte[] frame(long seq, byte[] payload, int off, int len) {
        byte[] out = new byte[HEADER + len];
        long v = seq;
        for (int i = 7; i >= 0; i--) { out[i] = (byte) v; v >>>= 8; }
        Bytes.putInt(out, 8, len);
        System.arraycopy(payload, off, out, HEADER, len);
        return out;
    }

    public static long seqOf(byte[] buf, int off) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (buf[off + i] & 0xFF);
        return v;
    }

    public static int lenOf(byte[] buf, int off) {
        return Bytes.getInt(buf, off + 8);
    }

    public static byte[] control(byte kind, long value) {
        byte[] body = new byte[9];
        body[0] = kind;
        long v = value;
        for (int i = 8; i >= 1; i--) { body[i] = (byte) v; v >>>= 8; }
        return frame(CTRL_SEQ, body, 0, body.length);
    }

    /**
     * ACK carrying the receiver's free delivery slots. Older peers read only the first
     * nine bytes and ignore the rest, so this stays backward compatible; peers that do
     * understand it stop sending before the receiver's queue grows without bound.
     */
    public static byte[] controlWithCredit(byte kind, long value, int credit) {
        byte[] body = new byte[17];
        body[0] = kind;
        long v = value;
        for (int i = 8; i >= 1; i--) { body[i] = (byte) v; v >>>= 8; }
        long c = credit;
        for (int i = 16; i >= 9; i--) { body[i] = (byte) c; c >>>= 8; }
        return frame(CTRL_SEQ, body, 0, body.length);
    }

    public static byte ctrlKindOf(byte[] payload) {
        return payload[0];
    }

    public static long ctrlValueOf(byte[] payload) {
        long v = 0;
        for (int i = 1; i <= 8; i++) v = (v << 8) | (payload[i] & 0xFF);
        return v;
    }

    /** Free delivery slots advertised by the peer, or -1 when it did not say. */
    public static int ctrlCreditOf(byte[] payload) {
        if (payload == null || payload.length < 17) return -1;
        long v = 0;
        for (int i = 9; i <= 16; i++) v = (v << 8) | (payload[i] & 0xFF);
        if (v < 0) return -1;
        return (int) Math.min(Integer.MAX_VALUE, v);
    }
}
