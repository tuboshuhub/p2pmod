package dev.p2pmod.support;

import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class Workers {

    private Workers() {}

    private static final ScheduledExecutorService POOL =
            Executors.newScheduledThreadPool(4, r -> {
                Thread t = new Thread(r, "p2p-worker");
                t.setDaemon(true);
                return t;
            });

    public static void run(Runnable task) {
        try {
            POOL.execute(task);
        } catch (RejectedExecutionException ignored) {
        }
    }

    public static void delay(Runnable task, long ms) {
        try {
            POOL.schedule(task, ms, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
        }
    }
}
