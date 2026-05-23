package io.sparkadvisor.core.predict;

import java.util.List;

/**
 * Result of the shuffle-partition "will it get faster or slower?" prediction for one stage.
 *
 * <p>This is a cost-model ESTIMATE. {@code assumptions} and {@code confidence} MUST be shown
 * to the user; {@code reversalNote} explains under what condition the conclusion would flip.
 *
 * @param stageId           the shuffle stage analyzed
 * @param currentPartitions effective partition count observed (post-AQE when applicable)
 * @param estCurrentMs       estimated stage time at the current partition count
 * @param recommendedPartitions partition count minimizing estimated time
 * @param estRecommendedMs   estimated stage time at the recommended count
 * @param direction         FASTER_IF_INCREASED / FASTER_IF_DECREASED / ALREADY_OPTIMAL / SKEW_LIMITED
 * @param curve             sampled (partitions, estMs) points for plotting
 * @param tunedKnob         the actual config knob to change (AQE-aware)
 * @param assumptions       human-readable assumptions behind the model
 * @param reversalNote      condition under which the conclusion would reverse
 */
public record ShufflePartitionPrediction(
        int stageId,
        int currentPartitions,
        long estCurrentMs,
        int recommendedPartitions,
        long estRecommendedMs,
        Direction direction,
        List<Point> curve,
        String tunedKnob,
        Confidence confidence,
        List<String> assumptions,
        String reversalNote) {

    public enum Direction {
        FASTER_IF_INCREASED,
        FASTER_IF_DECREASED,
        ALREADY_OPTIMAL,
        SKEW_LIMITED
    }

    /** One sampled point on the partitions-vs-time curve. */
    public record Point(int partitions, long estMs) {}

    /** Estimated speedup as a fraction (0.25 == 25% faster). Negative means slower. */
    public double estimatedSpeedup() {
        if (estCurrentMs <= 0) return 0.0;
        return (double) (estCurrentMs - estRecommendedMs) / (double) estCurrentMs;
    }
}
