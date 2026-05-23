package io.sparkadvisor.core.metrics;

import com.tdunning.math.stats.TDigest;

/**
 * Incremental builder that accumulates samples for one metric and produces a
 * {@link Distribution} without ever retaining the individual samples.
 *
 * <p>Backed by a t-digest for quantile estimation; min/max/sum/count are tracked
 * exactly. This keeps memory constant regardless of task count.
 *
 * <p>Not thread-safe: one builder is owned by a single stage accumulation and
 * fed from the (single-threaded) replay listener callback.
 */
public final class MetricDistributionBuilder {

    private static final double COMPRESSION = 100.0;

    private final TDigest digest = TDigest.createDigest(COMPRESSION);
    private long count = 0;
    private long sum = 0;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;

    /** Add one observation. Negative values are clamped to 0 (defensive). */
    public void add(long value) {
        long v = Math.max(value, 0L);
        digest.add((double) v);
        count++;
        sum += v;
        if (v < min) min = v;
        if (v > max) max = v;
    }

    public long count() {
        return count;
    }

    /** Materialize the summary. Safe to call multiple times. */
    public Distribution build() {
        if (count == 0) {
            return Distribution.EMPTY;
        }
        return new Distribution(
                count,
                min,
                quantile(0.25),
                quantile(0.50),
                quantile(0.75),
                quantile(0.90),
                max,
                sum);
    }

    private long quantile(double q) {
        // t-digest returns NaN for empty; guarded by count==0 above.
        double v = digest.quantile(q);
        return Math.round(v);
    }
}
