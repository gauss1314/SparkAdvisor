package io.sparkadvisor.core.predict;

import java.util.List;

/**
 * Result of the executor-scaling simulation: estimated wall clock at several executor-core
 * counts, used to find the point of diminishing returns.
 *
 * <p>Cost-model ESTIMATE; carries confidence and assumptions.
 *
 * @param currentCores     cores observed/derived for the current run
 * @param estCurrentMs      estimated wall clock at current cores
 * @param kneeCores        cores beyond which marginal speedup is negligible (the "knee")
 * @param curve            sampled (cores, estMs) points
 */
public record ExecutorScalingPrediction(
        int currentCores,
        long estCurrentMs,
        int kneeCores,
        List<Point> curve,
        Confidence confidence,
        List<String> assumptions) {

    /** One sampled point on the cores-vs-time curve. */
    public record Point(int cores, long estMs) {}
}
