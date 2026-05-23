package io.sparkadvisor.core.model;

import io.sparkadvisor.core.metrics.Distribution;

/**
 * Per-stage distributions of the task-level metrics SparkAdvisor cares about.
 * Each field is a {@link Distribution} over all tasks of the stage.
 */
public record TaskMetricStats(
        Distribution durationMs,
        Distribution shuffleReadBytes,
        Distribution shuffleWriteBytes,
        Distribution inputBytes,
        Distribution outputBytes,
        Distribution memorySpillBytes,
        Distribution diskSpillBytes,
        Distribution gcTimeMs,
        Distribution deserializeMs) {

    public static TaskMetricStats empty() {
        return new TaskMetricStats(
                Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY,
                Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY,
                Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY);
    }

    /** Total spill (memory + disk) bytes summed across all tasks. */
    public long totalSpillBytes() {
        return memorySpillBytes.sum() + diskSpillBytes.sum();
    }
}
