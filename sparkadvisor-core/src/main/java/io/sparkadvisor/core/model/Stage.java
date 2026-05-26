package io.sparkadvisor.core.model;

import java.util.List;
import java.util.Objects;

public final class Stage {
    private final int stageId;
    private final int attemptId;
    private final int numTasks;
    private final List<Integer> parentStageIds;
    private final long submissionTime;
    private final long firstTaskLaunchTime;
    private final long completionTime;
    private final long shuffleReadTotalBytes;
    private final long shuffleWriteTotalBytes;
    private final TaskMetricStats taskStats;

    public Stage(int stageId, int attemptId, int numTasks, List<Integer> parentStageIds, long submissionTime,
                 long firstTaskLaunchTime, long completionTime, long shuffleReadTotalBytes,
                 long shuffleWriteTotalBytes, TaskMetricStats taskStats) {
        this.stageId = stageId;
        this.attemptId = attemptId;
        this.numTasks = numTasks;
        this.parentStageIds = parentStageIds;
        this.submissionTime = submissionTime;
        this.firstTaskLaunchTime = firstTaskLaunchTime;
        this.completionTime = completionTime;
        this.shuffleReadTotalBytes = shuffleReadTotalBytes;
        this.shuffleWriteTotalBytes = shuffleWriteTotalBytes;
        this.taskStats = taskStats;
    }
    public int stageId() { return stageId; }
    public int attemptId() { return attemptId; }
    public int numTasks() { return numTasks; }
    public List<Integer> parentStageIds() { return parentStageIds; }
    public long submissionTime() { return submissionTime; }
    public long firstTaskLaunchTime() { return firstTaskLaunchTime; }
    public long completionTime() { return completionTime; }
    public long shuffleReadTotalBytes() { return shuffleReadTotalBytes; }
    public long shuffleWriteTotalBytes() { return shuffleWriteTotalBytes; }
    public TaskMetricStats taskStats() { return taskStats; }
    public long wallClockMs() { return (submissionTime <= 0 || completionTime <= 0) ? 0 : completionTime - submissionTime; }
    public long schedulingDelayMs() { return (submissionTime <= 0 || firstTaskLaunchTime <= 0) ? 0 : Math.max(0, firstTaskLaunchTime - submissionTime); }
    @Override public boolean equals(Object o) { if (this == o) return true; if (!(o instanceof Stage)) return false; Stage stage = (Stage) o; return stageId == stage.stageId && attemptId == stage.attemptId && numTasks == stage.numTasks && submissionTime == stage.submissionTime && firstTaskLaunchTime == stage.firstTaskLaunchTime && completionTime == stage.completionTime && shuffleReadTotalBytes == stage.shuffleReadTotalBytes && shuffleWriteTotalBytes == stage.shuffleWriteTotalBytes && Objects.equals(parentStageIds, stage.parentStageIds) && Objects.equals(taskStats, stage.taskStats); }
    @Override public int hashCode() { return Objects.hash(stageId, attemptId, numTasks, parentStageIds, submissionTime, firstTaskLaunchTime, completionTime, shuffleReadTotalBytes, shuffleWriteTotalBytes, taskStats); }
}
