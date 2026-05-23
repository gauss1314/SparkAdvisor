package io.sparkadvisor.core.analyze;

import io.sparkadvisor.core.model.Stage;

/**
 * A per-stage analytical view derived from a {@link Stage}, carrying the hard metrics
 * the report surfaces. Pure data; no Spark types.
 *
 * @param skewRatio        task duration max/median (0 if not computable)
 * @param shuffleSkewRatio shuffle-read max/median (0 if not computable)
 * @param spillBytes       total memory+disk spill across tasks
 * @param gcRatio          sum(gcTime)/sum(taskDuration) (0..1)
 * @param schedulingDelayMs firstTaskLaunch - submissionTime
 * @param maxTaskMs        longest single task = lower bound under infinite parallelism
 * @param inputBytes       total input bytes read across tasks (source read, not shuffle)
 * @param medianInputBytesPerTask median per-task input bytes (for small-files detection)
 */
public record StageAnalysis(
        int stageId,
        int numTasks,
        long wallClockMs,
        long maxTaskMs,
        long medianTaskMs,
        long totalTaskTimeMs,
        double skewRatio,
        double shuffleSkewRatio,
        long shuffleReadBytes,
        long shuffleWriteBytes,
        long spillBytes,
        double gcRatio,
        long schedulingDelayMs,
        long inputBytes,
        long medianInputBytesPerTask) {

    public static StageAnalysis from(Stage s) {
        var dur = s.taskStats().durationMs();
        var sr = s.taskStats().shuffleReadBytes();
        var in = s.taskStats().inputBytes();
        long totalTaskMs = dur.sum();
        long gcSum = s.taskStats().gcTimeMs().sum();
        double gcRatio = totalTaskMs == 0 ? 0.0 : (double) gcSum / (double) totalTaskMs;
        return new StageAnalysis(
                s.stageId(),
                s.numTasks(),
                s.wallClockMs(),
                dur.max(),
                dur.median(),
                totalTaskMs,
                dur.skewRatio(),
                sr.skewRatio(),
                s.shuffleReadTotalBytes(),
                s.shuffleWriteTotalBytes(),
                s.taskStats().totalSpillBytes(),
                gcRatio,
                s.schedulingDelayMs(),
                in.sum(),
                in.median());
    }
}
