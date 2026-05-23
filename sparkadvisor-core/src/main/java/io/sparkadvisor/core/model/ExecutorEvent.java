package io.sparkadvisor.core.model;

/**
 * An executor lifecycle event captured during replay, used to reconstruct how many cores were
 * available over time (for accurate utilization, replacing the config-based approximation).
 *
 * @param timeMs    event time (ms epoch)
 * @param cores     number of cores this executor contributes
 * @param added     true for ExecutorAdded, false for ExecutorRemoved
 */
public record ExecutorEvent(long timeMs, int cores, boolean added) {
}
