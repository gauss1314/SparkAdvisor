package io.sparkadvisor.predictor.costmodel;

/**
 * Analytical cost model for a single shuffle stage, per design doc §8.1.
 *
 * <p>For a candidate partition count {@code p}:
 * <pre>
 *   b(p) = B / p                         bytes per task
 *   t(p) = o + b(p)/r + spillPenalty     time per task (ms)
 *   w(p) = ceil(p / C)                   number of waves given C cores
 *   T(p) = w(p) * t(p)                   estimated stage time
 * </pre>
 * where {@code o} (fixed per-task overhead, ms) and {@code r} (processing throughput,
 * bytes/ms) are fit from the observed run, {@code M} is the per-task memory budget (bytes),
 * and {@code spillPenalty} is non-zero only when {@code b(p) > M}.
 *
 * <p>All estimates are approximate; callers must surface confidence and assumptions.
 */
public final class ShuffleCostModel {
    private final long totalShuffleBytes;
    private final int cores;
    private final double fixedOverheadMs;
    private final double throughputBytesPerMs;
    private final long perTaskMemoryBudgetBytes;

    public ShuffleCostModel(long totalShuffleBytes, int cores, double fixedOverheadMs, double throughputBytesPerMs, long perTaskMemoryBudgetBytes){
        this.totalShuffleBytes=totalShuffleBytes; this.cores=cores; this.fixedOverheadMs=fixedOverheadMs; this.throughputBytesPerMs=throughputBytesPerMs; this.perTaskMemoryBudgetBytes=perTaskMemoryBudgetBytes;
    }
    public long totalShuffleBytes(){return totalShuffleBytes;} public int cores(){return cores;} public double fixedOverheadMs(){return fixedOverheadMs;} public double throughputBytesPerMs(){return throughputBytesPerMs;} public long perTaskMemoryBudgetBytes(){return perTaskMemoryBudgetBytes;}

    /** Estimated stage time (ms) at partition count p. */
    public long estimateMs(int p) {
        if (p <= 0) return Long.MAX_VALUE;
        double b = (double) totalShuffleBytes / (double) p;
        double t = fixedOverheadMs + b / Math.max(1e-9, throughputBytesPerMs) + spillPenaltyMs(b);
        long waves = (long) Math.ceil((double) p / (double) Math.max(1, cores));
        return Math.round(waves * t);
    }

    /**
     * Spill penalty (ms) when a task's bytes exceed the memory budget. Modeled as the extra
     * cost of writing+reading the overflow once at the same throughput (a deliberately
     * conservative, simple model — refine with measured spill rates later).
     */
    private double spillPenaltyMs(double bytesPerTask) {
        if (perTaskMemoryBudgetBytes <= 0 || bytesPerTask <= perTaskMemoryBudgetBytes) {
            return 0.0;
        }
        double overflow = bytesPerTask - perTaskMemoryBudgetBytes;
        // write overflow then read it back => 2x at throughput r
        return 2.0 * overflow / Math.max(1e-9, throughputBytesPerMs);
    }

    /**
     * Fit {@code o} and {@code r} from one observed operating point.
     *
     * <p>With a single point (observed bytes/task and time/task) we cannot separate fixed
     * overhead from throughput uniquely, so we assume a fixed-overhead fraction {@code fixedFrac}
     * of the observed task time and attribute the remainder to throughput. This is a coarse
     * fit; with more sampled points a regression would be used (future work).
     *
     * @param observedBytesPerTask median bytes processed per task in the observed run
     * @param observedMsPerTask     median task time in the observed run
     * @param fixedFrac             fraction of observed task time assumed to be fixed overhead (0..1)
     */
    public static double[] fitOR(double observedBytesPerTask, double observedMsPerTask, double fixedFrac) {
        double o = observedMsPerTask * fixedFrac;
        double variable = Math.max(1e-9, observedMsPerTask - o);
        double r = observedBytesPerTask <= 0 ? 1.0 : observedBytesPerTask / variable;
        return new double[]{o, r};
    }
}
