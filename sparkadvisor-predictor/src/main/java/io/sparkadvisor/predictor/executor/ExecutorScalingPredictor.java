package io.sparkadvisor.predictor.executor;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.predict.Confidence;
import io.sparkadvisor.core.predict.ExecutorScalingPrediction;

import java.util.ArrayList;
import java.util.List;

/**
 * Estimates wall clock at several core counts to find the point of diminishing returns
 * (design §8.3). For each candidate core count C, each stage is modeled as:
 * <pre>
 *   stageTime(C) = max( maxTaskMs,  ceil(numTasks / C) * meanTaskMs )
 * </pre>
 * i.e. you cannot beat the longest single task (skew), and otherwise you pay ceil(waves)
 * times the average task time. Stage times are summed along the critical path ordering
 * (here approximated by summing per-stage estimates, since the aggregator already gives a
 * critical-path-consistent stage set). This is a coarse but useful ROI curve.
 */
public final class ExecutorScalingPredictor {

    private static final double KNEE_MARGINAL_GAIN = 0.05; // <5% extra speedup => past the knee

    public ExecutorScalingPrediction predict(SqlAnalysis sql, int currentCores) {
        List<Integer> candidates = candidateCores(currentCores);
        List<ExecutorScalingPrediction.Point> curve = new ArrayList<>();
        for (int c : candidates) {
            curve.add(new ExecutorScalingPrediction.Point(c, estimateWallMs(sql, c)));
        }
        long estCurrent = estimateWallMs(sql, currentCores);
        int knee = findKnee(curve);

        return new ExecutorScalingPrediction(
                currentCores, estCurrent, knee, curve, Confidence.MEDIUM,
                List.of(
                        "Each stage modeled as max(longestTask, waves * avgTask).",
                        "Skew caps the achievable speedup (longest task is irreducible).",
                        "Assumes adding cores does not change data layout or shuffle cost."));
    }

    private long estimateWallMs(SqlAnalysis sql, int cores) {
        long total = 0;
        for (StageAnalysis st : sql.stages()) {
            total += stageTime(st, cores);
        }
        return total;
    }

    private long stageTime(StageAnalysis st, int cores) {
        if (st.numTasks() <= 0) return st.wallClockMs();
        double meanTask = (double) st.totalTaskTimeMs() / (double) st.numTasks();
        long waves = (long) Math.ceil((double) st.numTasks() / (double) Math.max(1, cores));
        long balanced = Math.round(waves * meanTask);
        // Cannot go below the single longest task (skew floor).
        return Math.max(st.maxTaskMs(), balanced);
    }

    /** Core counts: fractions and multiples of current. */
    private List<Integer> candidateCores(int current) {
        java.util.LinkedHashSet<Integer> set = new java.util.LinkedHashSet<>();
        for (double f : new double[]{0.25, 0.5, 1.0, 1.5, 2.0, 4.0}) {
            int v = (int) Math.round(current * f);
            if (v >= 1) set.add(v);
        }
        return new ArrayList<>(set);
    }

    /**
     * The knee is the smallest core count beyond which doubling cores yields less than
     * KNEE_MARGINAL_GAIN relative speedup. Curve must be sorted ascending by cores.
     */
    private int findKnee(List<ExecutorScalingPrediction.Point> curve) {
        curve.sort((a, b) -> Integer.compare(a.cores(), b.cores()));
        for (int i = 0; i < curve.size() - 1; i++) {
            long here = curve.get(i).estMs();
            long next = curve.get(i + 1).estMs();
            if (here <= 0) continue;
            double gain = (double) (here - next) / (double) here;
            if (gain < KNEE_MARGINAL_GAIN) {
                return curve.get(i).cores();
            }
        }
        return curve.get(curve.size() - 1).cores();
    }
}
