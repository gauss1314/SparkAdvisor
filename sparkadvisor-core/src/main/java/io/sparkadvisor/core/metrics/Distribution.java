package io.sparkadvisor.core.metrics;

/**
 * Summary statistics for a single metric measured across many tasks of a stage.
 *
 * <p>Produced incrementally by {@link MetricDistributionBuilder} so that we never
 * retain per-task records (a single complex SQL can emit millions of tasks).
 *
 * <p>All byte/time values use the raw unit of the underlying Spark metric
 * (bytes for byte metrics, milliseconds for time metrics).
 */
public record Distribution(
        long count,
        long min,
        long p25,
        long median,
        long p75,
        long p90,
        long max,
        long sum) {

    /** An empty distribution (no samples observed). */
    public static final Distribution EMPTY = new Distribution(0, 0, 0, 0, 0, 0, 0, 0);

    public double mean() {
        return count == 0 ? 0.0 : (double) sum / (double) count;
    }

    /**
     * Skew ratio max/median. Returns 0 when median is 0 (degenerate / no samples)
     * so callers can treat 0 as "not applicable".
     */
    public double skewRatio() {
        return median == 0 ? 0.0 : (double) max / (double) median;
    }
}
