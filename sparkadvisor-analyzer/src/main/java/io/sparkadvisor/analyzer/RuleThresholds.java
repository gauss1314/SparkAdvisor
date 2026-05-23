package io.sparkadvisor.analyzer;

/**
 * Centralized, tunable thresholds for the rule engine. This is the single source of truth
 * for rule trigger points (the HTML renderer must not carry its own copies).
 *
 * <p>Defaults follow the design doc §7.3. Construct via {@link #defaults()} or the builder
 * for overrides.
 */
public record RuleThresholds(
        double skewRatioWarn,        // task duration max/median above this -> skew
        double skewRatioCritical,
        double shuffleSkewWarn,      // shuffle-read max/median above this -> skew
        double spillRatioWarn,       // (spill)/(input) above this -> excessive spill
        double gcRatioWarn,          // sum(gc)/sum(taskTime) above this -> GC pressure
        double coreUtilLow,          // utilization below this -> under-parallelized
        long smallTaskMedianMs,      // median task below this AND many tasks -> over-parallel
        int overParallelMinTasks,    // task count above this to consider "over-parallel"
        long smallInputPerTaskBytes, // per-task input below this with many tasks -> small files
        double schedulingDelayRatioWarn // schedulingDelay/wallClock above this -> scheduling wait
) {

    public static RuleThresholds defaults() {
        return new RuleThresholds(
                5.0,        // skewRatioWarn
                10.0,       // skewRatioCritical
                5.0,        // shuffleSkewWarn
                0.5,        // spillRatioWarn
                0.10,       // gcRatioWarn
                0.40,       // coreUtilLow
                200L,       // smallTaskMedianMs
                2000,       // overParallelMinTasks
                4L * 1024 * 1024,  // smallInputPerTaskBytes (4 MB)
                0.30        // schedulingDelayRatioWarn
        );
    }
}
