package dev.p2pmod.core;

import java.security.SecureRandom;

public final class RoomId {

    private RoomId() {}

    public static final int LENGTH = 4;

    public static final String DEFAULT = "default";

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private static final SecureRandom RNG = new SecureRandom();

    public static boolean isValid(String room) {
        if (room == null || room.length() != LENGTH) return false;
        for (int i = 0; i < LENGTH; i++) {
            if (ALPHABET.indexOf(room.charAt(i)) < 0) return false;
        }
        return true;
    }

    public static boolean acceptable(String room) {
        return room == null || room.isBlank() || DEFAULT.equals(room) || isValid(room);
    }

    public static String random() {
        StringBuilder sb = new StringBuilder(LENGTH);
        for (int i = 0; i < LENGTH; i++) {
            sb.append(ALPHABET.charAt(RNG.nextInt(ALPHABET.length())));
        }
        return sb.toString();
    }

    public static String normalize(String room) {
        if (room == null || room.isBlank()) return DEFAULT;
        return room.trim();
    }
}
