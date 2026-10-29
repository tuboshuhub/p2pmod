package dev.p2pmod.core;

import java.io.IOException;

public class RetryableIOException extends IOException {

    public static final long WINDOW_FULL_BUDGET_MS = 15_000;

    public static final long NOT_READY_BUDGET_MS = 3_000;

    private final long budgetMs;

    public RetryableIOException(String message, long budgetMs) {
        super(message);
        this.budgetMs = budgetMs;
    }

    public long budgetMs() {
        return budgetMs;
    }
}
