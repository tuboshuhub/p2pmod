package dev.p2pmod.support;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class Scheduler {

    private Scheduler() {}

    private static final Set<Runnable> TASKS = ConcurrentHashMap.newKeySet();

    private static final ScheduledExecutorService EXEC =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "p2p-scheduler");
                t.setDaemon(true);
                return t;
            });

    static {
        EXEC.scheduleWithFixedDelay(Scheduler::beat, 100, 100, TimeUnit.MILLISECONDS);
    }

    private static void beat() {
        for (Runnable r : TASKS) {
            try {
                r.run();
            } catch (Throwable t) {
                Trace.debug("Heartbeat task failed: {}", t.toString());
            }
        }
    }

    public static void register(Runnable task) {
        TASKS.add(task);
    }

    public static void unregister(Runnable task) {
        TASKS.remove(task);
    }
}
