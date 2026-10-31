package dev.p2pmod.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Forward Error Correction over GF(2^8).
 *
 * Treats a block of K source fragments as K rows of a systematic Reed-Solomon
 * codeword of size (K + R): the first K codeword symbols are the sources
 * verbatim, and the last R are parity symbols computed from a Vandermonde
 * generator. Any K of the (K+R) symbols are enough to reconstruct all K
 * sources, so up to R lost fragments per block are recovered without any
 * retransmission.
 *
 * This is the layer that turns a lossy UDP link into one the application
 * perceives as (almost) loss-free: a few percent of random loss stops
 * triggering retransmission storms.
 */
public final class Fec {

    private Fec() {}

    /** Primitive polynomial x^8 + x^4 + x^3 + x^2 + 1 (0x11D). */
    private static final int PRIM = 0x11D;

    private static final int[] EXP = new int[512];
    private static final int[] LOG = new int[256];

    static {
        int x = 1;
        for (int i = 0; i < 255; i++) {
            EXP[i] = x;
            LOG[x] = i;
            x <<= 1;
            if ((x & 0x100) != 0) x ^= PRIM;
        }
        for (int i = 255; i < 512; i++) EXP[i] = EXP[i - 255];
        LOG[0] = 0;
    }

    private static int gmul(int a, int b) {
        if (a == 0 || b == 0) return 0;
        return EXP[LOG[a] + LOG[b]];
    }

    private static int ginv(int a) {
        if (a == 0) return 0;
        return EXP[255 - LOG[a]];
    }

    /** Vandermonde entry evaluated at codeword index `row` and source column `col`. */
    private static int vmn(int row, int col) {
        int e = (row * col) % 255;
        if (e < 0) e += 255;
        return EXP[e];
    }

    /** Cached systematic parity generator P (R x K) keyed by (K, R). */
    private static final Map<Long, int[][]> PARITY_GEN = new ConcurrentHashMap<>();

    private static int[][] parityGen(int K, int R) {
        long key = ((long) K << 32) | (R & 0xFFFFFFFFL);
        int[][] cached = PARITY_GEN.get(key);
        if (cached != null) return cached;

        int[][] vtop = new int[K][K];
        for (int r = 0; r < K; r++)
            for (int c = 0; c < K; c++)
                vtop[r][c] = vmn(r, c);
        int[][] vtopInv = invert(vtop, K);

        int[][] p = new int[R][K];
        for (int i = 0; i < R; i++) {
            for (int c = 0; c < K; c++) {
                int acc = 0;
                for (int j = 0; j < K; j++)
                    acc ^= gmul(vmn(K + i, j), vtopInv[j][c]);
                p[i][c] = acc;
            }
        }
        PARITY_GEN.put(key, p);
        return p;
    }

    /**
     * Produce R parity rows for K source rows.
     *
     * @param src length-K array, each a byte[] of identical length L.
     * @param K   number of source rows (1..255).
     * @param R   number of parity rows (>=0).
     * @return R byte[] rows of length L (empty array if R == 0).
     */
    public static byte[][] encode(byte[][] src, int K, int R) {
        if (R <= 0) return new byte[0][];
        int L = src[0].length;
        int[][] P = parityGen(K, R);
        byte[][] parity = new byte[R][L];
        for (int i = 0; i < R; i++) {
            for (int c = 0; c < L; c++) {
                int acc = 0;
                for (int j = 0; j < K; j++) {
                    int s = src[j][c] & 0xFF;
                    if (s != 0) acc ^= gmul(s, P[i][j]);
                }
                parity[i][c] = (byte) acc;
            }
        }
        return parity;
    }

    /**
     * Recover the K source rows from a (K+R)-symbol codeword where some symbols
     * are missing.
     *
     * @param syms     length-(K+R); null where a symbol is missing.
     * @param present  length-(K+R); true where syms[idx] is available.
     * @param K        number of source rows.
     * @param R        number of parity rows.
     * @return K recovered source rows (length L), or null if fewer than K symbols present.
     */
    public static byte[][] decode(byte[][] syms, boolean[] present, int K, int R) {
        int total = K + R;
        int L = 0;
        int chosen = 0;
        int[] rows = new int[K];
        for (int i = 0; i < total && chosen < K; i++) {
            if (present[i] && syms[i] != null) {
                if (L == 0) L = syms[i].length;
                rows[chosen++] = i;
            }
        }
        if (chosen < K) return null;

        int[][] P = parityGen(K, R);
        int[][] A = new int[K][K];
        for (int r = 0; r < K; r++) {
            int idx = rows[r];
            if (idx < K) A[r][idx] = 1;               // systematic: source row is identity
            else for (int c = 0; c < K; c++) A[r][c] = P[idx - K][c];
        }
        int[][] Ainv = invert(A, K);
        if (Ainv == null) return null;

        byte[][] out = new byte[K][L];
        for (int j = 0; j < K; j++) {
            for (int c = 0; c < L; c++) {
                int acc = 0;
                for (int k = 0; k < K; k++) {
                    int s = syms[rows[k]][c] & 0xFF;
                    if (s != 0) acc ^= gmul(s, Ainv[j][k]);
                }
                out[j][c] = (byte) acc;
            }
        }
        return out;
    }

    /** Invert an NxN matrix over GF(2^8) via Gauss-Jordan elimination. */
    private static int[][] invert(int[][] m, int n) {
        int[][] a = new int[n][2 * n];
        for (int i = 0; i < n; i++) {
            System.arraycopy(m[i], 0, a[i], 0, n);
            a[i][n + i] = 1;
        }
        for (int col = 0; col < n; col++) {
            int piv = -1;
            for (int r = col; r < n; r++) {
                if (a[r][col] != 0) { piv = r; break; }
            }
            if (piv < 0) return null;
            if (piv != col) { int[] t = a[piv]; a[piv] = a[col]; a[col] = t; }
            int inv = ginv(a[col][col]);
            for (int j = 0; j < 2 * n; j++) a[col][j] = gmul(a[col][j], inv);
            for (int r = 0; r < n; r++) {
                if (r == col) continue;
                int f = a[r][col];
                if (f == 0) continue;
                for (int j = 0; j < 2 * n; j++) a[r][j] ^= gmul(a[col][j], f);
            }
        }
        int[][] inv = new int[n][n];
        for (int i = 0; i < n; i++) System.arraycopy(a[i], n, inv[i], 0, n);
        return inv;
    }

    /** Random data, drop up to R symbols, verify bit-exact recovery. */
    public static boolean selfTest(int K, int R, int L, int trials) {
        Random rnd = new Random(0x5EED1234L);
        for (int t = 0; t < trials; t++) {
            byte[][] src = new byte[K][L];
            for (int i = 0; i < K; i++) rnd.nextBytes(src[i]);
            byte[][] parity = encode(src, K, R);
            byte[][] syms = new byte[K + R][];
            boolean[] present = new boolean[K + R];
            for (int i = 0; i < K; i++) { syms[i] = src[i]; present[i] = true; }
            for (int i = 0; i < R; i++) { syms[K + i] = parity[i]; present[K + i] = true; }
            List<Integer> idx = new ArrayList<>();
            for (int i = 0; i < K + R; i++) idx.add(i);
            Collections.shuffle(idx, rnd);
            int drop = Math.min(R, rnd.nextInt(R + 1));
            for (int d = 0; d < drop; d++) {
                int k = idx.get(d);
                syms[k] = null;
                present[k] = false;
            }
            byte[][] rec = decode(syms, present, K, R);
            if (rec == null) return false;
            for (int i = 0; i < K; i++)
                if (!Arrays.equals(rec[i], src[i])) return false;
        }
        return true;
    }

    public static void main(String[] args) {
        int[] ks = {4, 8, 10, 12, 16, 20, 32, 48};
        int[] rs = {1, 2, 3, 4, 6};
        boolean ok = true;
        for (int K : ks) {
            for (int R : rs) {
                if (R > K) continue;
                boolean r = selfTest(K, R, 1300, 80);
                System.out.println("K=" + K + " R=" + R + " L=1300 trials=80 -> " + (r ? "OK" : "FAIL"));
                ok &= r;
            }
        }
        System.out.println(ok ? "ALL OK" : "SOME FAILED");
        System.exit(ok ? 0 : 1);
    }
}
