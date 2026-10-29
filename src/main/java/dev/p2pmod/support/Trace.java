package dev.p2pmod.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class Trace {

    private Trace() {}

    public static final Logger LOG = LoggerFactory.getLogger("p2pmod");

    private static final long QUIET_WINDOW_MS = 5_000;

    private static final int PURGE_THRESHOLD = 512;

    private static final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
    private static final Map<String, Long> repeats = new ConcurrentHashMap<>();
    private static final AtomicLong calls = new AtomicLong();

    private static volatile boolean verbose = false;

    public static void setVerbose(boolean v) { verbose = v; }
    public static boolean verbose() { return verbose; }

    public static void periodic(String format, Object... args) {
        String key = (args.length == 0) ? format : format + '\u0000' + args[0];
        long now = System.currentTimeMillis();
        Long prev = lastSeen.get(key);
        if (prev != null && now - prev < QUIET_WINDOW_MS) {
            repeats.merge(key, 1L, Long::sum);
            return;
        }

        Long dup = repeats.remove(key);
        long times = (dup == null) ? 0 : dup;
        lastSeen.put(key, now);
        LOG.info(format + (times > 0 ? " ({} repeats suppressed)" : ""), append(args, times));
        purgeOccasionally(now);
    }

    private static void purgeOccasionally(long now) {
        if ((calls.incrementAndGet() & 0xFF) != 0) return;
        if (lastSeen.size() < PURGE_THRESHOLD) return;
        lastSeen.values().removeIf(seen -> now - seen > QUIET_WINDOW_MS * 2);
        repeats.keySet().removeIf(k -> !lastSeen.containsKey(k));
    }

    private static Object[] append(Object[] args, long times) {
        if (times <= 0) return args;
        Object[] out = new Object[args.length + 1];
        System.arraycopy(args, 0, out, 0, args.length);
        out[args.length] = times;
        return out;
    }

    public static void info(String format, Object... args) {
        LOG.info(format, args);
    }

    public static void warn(String format, Object... args) {
        LOG.warn(format, args);
    }

    public static void error(String format, Object... args) {
        LOG.error(format, args);
    }

    public static void debug(String format, Object... args) {
        if (verbose) LOG.info("[debug] " + format, args);
    }
}
