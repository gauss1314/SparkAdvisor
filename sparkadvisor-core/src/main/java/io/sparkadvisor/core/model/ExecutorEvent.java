package io.sparkadvisor.core.model;

import java.util.Objects;

/**
 * An executor lifecycle event captured during replay, used to reconstruct how many cores were
 * available over time (for accurate utilization, replacing the config-based approximation).
 */
public final class ExecutorEvent {
    private final long timeMs;
    private final int cores;
    private final boolean added;

    public ExecutorEvent(long timeMs, int cores, boolean added) {
        this.timeMs = timeMs;
        this.cores = cores;
        this.added = added;
    }

    public long timeMs() { return timeMs; }
    public int cores() { return cores; }
    public boolean added() { return added; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExecutorEvent)) return false;
        ExecutorEvent that = (ExecutorEvent) o;
        return timeMs == that.timeMs && cores == that.cores && added == that.added;
    }

    @Override
    public int hashCode() { return Objects.hash(timeMs, cores, added); }

    @Override
    public String toString() {
        return "ExecutorEvent{" + "timeMs=" + timeMs + ", cores=" + cores + ", added=" + added + '}';
    }
}
