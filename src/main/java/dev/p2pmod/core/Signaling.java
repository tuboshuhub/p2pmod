package dev.p2pmod.core;

import java.nio.charset.StandardCharsets;

public final class Signaling {

    private Signaling() {}

    public static final byte MAGIC_0 = 'P';
    public static final byte MAGIC_1 = '2';
    public static final int DEFAULT_PORT = 34777;

    public static final String DEFAULT_ROOM = RoomId.DEFAULT;

    public static final int OP_REGISTER_HOST   = 0x01;
    public static final int OP_REGISTER_CLIENT = 0x02;
    public static final int OP_PEER_INFO       = 0x03;
    public static final int OP_RELAY           = 0x04;
    public static final int OP_KEEPALIVE       = 0x05;
    public static final int OP_REGISTER_OK     = 0x06;
    public static final int OP_ERROR           = 0x07;
    public static final int OP_PEER_REQ        = 0x08;
    public static final int OP_UNREGISTER_HOST = 0x09;
    public static final int OP_UNREGISTER_CLIENT = 0x0A;

    public static final int OP_EXIT = 0x0B;

    private static final int CTRL_HEADER = 7;
    private static final int DATA_HEADER = 11;

    public static byte[] control(int op, int token, byte[] body) {
        byte[] out = new byte[CTRL_HEADER + body.length];
        out[0] = MAGIC_0;
        out[1] = MAGIC_1;
        out[2] = (byte) op;
        Bytes.putInt(out, 3, token);
        System.arraycopy(body, 0, out, CTRL_HEADER, body.length);
        return out;
    }

    public static byte[] forward(int fromToken, int toToken, byte[] body) {
        byte[] out = new byte[DATA_HEADER + body.length];
        out[0] = MAGIC_0;
        out[1] = MAGIC_1;
        out[2] = (byte) OP_RELAY;
        Bytes.putInt(out, 3, fromToken);
        Bytes.putInt(out, 7, toToken);
        System.arraycopy(body, 0, out, DATA_HEADER, body.length);
        return out;
    }

    public static boolean isSignaling(byte[] buf, int len) {
        return len >= 3 && buf[0] == MAGIC_0 && buf[1] == MAGIC_1;
    }

    public static int opOf(byte[] buf) {
        return buf[2] & 0xFF;
    }

    public static int tokenOf(byte[] buf) {
        return Bytes.getInt(buf, 3);
    }

    public static int toTokenOf(byte[] buf) {
        return Bytes.getInt(buf, 7);
    }

    public static byte[] controlBodyOf(byte[] buf, int len) {
        if (len <= CTRL_HEADER) return new byte[0];
        byte[] b = new byte[len - CTRL_HEADER];
        System.arraycopy(buf, CTRL_HEADER, b, 0, b.length);
        return b;
    }

    public static byte[] forwardBodyOf(byte[] buf, int len) {
        if (len <= DATA_HEADER) return new byte[0];
        byte[] b = new byte[len - DATA_HEADER];
        System.arraycopy(buf, DATA_HEADER, b, 0, b.length);
        return b;
    }

    public static byte[] registerBody(String room, int connId) {
        byte[] r = room.getBytes(StandardCharsets.UTF_8);
        if (r.length > 255) throw new IllegalArgumentException("room too long");
        byte[] out = new byte[1 + r.length + 4];
        out[0] = (byte) r.length;
        System.arraycopy(r, 0, out, 1, r.length);
        Bytes.putInt(out, 1 + r.length, connId);
        return out;
    }

    public static String roomOf(byte[] body) {
        if (body.length < 1) return DEFAULT_ROOM;
        int rl = body[0] & 0xFF;
        if (rl > body.length - 1) rl = body.length - 1;
        return new String(body, 1, rl, StandardCharsets.UTF_8);
    }

    public static int connIdOf(byte[] body) {
        int rl = body.length >= 1 ? (body[0] & 0xFF) : 0;
        if (body.length - (1 + rl) < 4) return 0;
        return Bytes.getInt(body, 1 + rl);
    }

    public static byte[] peerInfoBody(int peerToken, java.net.InetSocketAddress addr) {
        byte[] out = new byte[4 + Bytes.ADDR_LEN];
        Bytes.putInt(out, 0, peerToken);
        Bytes.putAddr(out, 4, addr);
        return out;
    }

    public record PeerInfo(int token, java.net.InetSocketAddress addr) {}

    public static PeerInfo parsePeerInfo(byte[] body, int len) {
        if (len < 4 + Bytes.ADDR_LEN) throw new IllegalArgumentException("bad PEER_INFO len=" + len);
        return new PeerInfo(Bytes.getInt(body, 0), Bytes.getAddr(body, 4));
    }

    public static byte[] registerOkBody(int token, String room, boolean canForward) {
        byte[] r = (room == null ? "" : room).getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[6 + r.length];
        Bytes.putInt(out, 0, token);
        out[4] = (byte) Math.min(255, r.length);
        System.arraycopy(r, 0, out, 5, r.length);
        out[5 + r.length] = (byte) (canForward ? 1 : 0);
        return out;
    }

    public static boolean canForwardOfRegisterOk(byte[] body) {
        if (body.length < 5) return true;
        int idx = 5 + (body[4] & 0xFF);
        if (body.length <= idx) return true;
        return body[idx] != 0;
    }

    public static int tokenOfRegisterOk(byte[] body) {
        return body.length >= 4 ? Bytes.getInt(body, 0) : 0;
    }

    public static String roomOfRegisterOk(byte[] body) {
        if (body.length < 5) return null;
        int rl = body[4] & 0xFF;
        if (body.length < 5 + rl) return null;
        return new String(body, 5, rl, StandardCharsets.UTF_8);
    }

    public static byte[] intBody(int v) {
        byte[] b = new byte[4];
        Bytes.putInt(b, 0, v);
        return b;
    }

    public static byte[] intBody(int v, boolean canForward) {
        byte[] b = new byte[5];
        Bytes.putInt(b, 0, v);
        b[4] = (byte) (canForward ? 1 : 0);
        return b;
    }

    public static boolean canForwardOf(byte[] body) {
        return body.length < 5 || body[4] != 0;
    }

    public static int intBodyValue(byte[] body) {
        return body.length >= 4 ? Bytes.getInt(body, 0) : 0;
    }

    public static byte[] textBody(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String textBodyValue(byte[] body) {
        return new String(body, StandardCharsets.UTF_8);
    }

    public static String opName(int op) {
        return switch (op) {
            case OP_REGISTER_HOST   -> "REGISTER_HOST";
            case OP_REGISTER_CLIENT -> "REGISTER_CLIENT";
            case OP_PEER_INFO       -> "PEER_INFO";
            case OP_RELAY           -> "RELAY";
            case OP_KEEPALIVE       -> "KEEPALIVE";
            case OP_REGISTER_OK     -> "REGISTER_OK";
            case OP_ERROR           -> "ERROR";
            case OP_PEER_REQ        -> "PEER_REQ";
            case OP_UNREGISTER_HOST -> "UNREGISTER_HOST";
            case OP_UNREGISTER_CLIENT -> "UNREGISTER_CLIENT";
            default -> "OP_" + op;
        };
    }
}
