package io.sparkadvisor.core.model;

import io.sparkadvisor.core.metrics.Distribution;
import java.util.Objects;

public final class TaskMetricStats {
    private final Distribution durationMs;
    private final Distribution shuffleReadBytes;
    private final Distribution shuffleWriteBytes;
    private final Distribution inputBytes;
    private final Distribution outputBytes;
    private final Distribution memorySpillBytes;
    private final Distribution diskSpillBytes;
    private final Distribution gcTimeMs;
    private final Distribution deserializeMs;

    public TaskMetricStats(Distribution durationMs, Distribution shuffleReadBytes, Distribution shuffleWriteBytes,
                           Distribution inputBytes, Distribution outputBytes, Distribution memorySpillBytes,
                           Distribution diskSpillBytes, Distribution gcTimeMs, Distribution deserializeMs) {
        this.durationMs = durationMs;
        this.shuffleReadBytes = shuffleReadBytes;
        this.shuffleWriteBytes = shuffleWriteBytes;
        this.inputBytes = inputBytes;
        this.outputBytes = outputBytes;
        this.memorySpillBytes = memorySpillBytes;
        this.diskSpillBytes = diskSpillBytes;
        this.gcTimeMs = gcTimeMs;
        this.deserializeMs = deserializeMs;
    }
    public Distribution durationMs() { return durationMs; }
    public Distribution shuffleReadBytes() { return shuffleReadBytes; }
    public Distribution shuffleWriteBytes() { return shuffleWriteBytes; }
    public Distribution inputBytes() { return inputBytes; }
    public Distribution outputBytes() { return outputBytes; }
    public Distribution memorySpillBytes() { return memorySpillBytes; }
    public Distribution diskSpillBytes() { return diskSpillBytes; }
    public Distribution gcTimeMs() { return gcTimeMs; }
    public Distribution deserializeMs() { return deserializeMs; }

    public static TaskMetricStats empty() {
        return new TaskMetricStats(Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY,
                Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY);
    }
    public long totalSpillBytes() { return memorySpillBytes.sum() + diskSpillBytes.sum(); }
    @Override public boolean equals(Object o) { if (this == o) return true; if (!(o instanceof TaskMetricStats)) return false; TaskMetricStats that = (TaskMetricStats) o; return Objects.equals(durationMs, that.durationMs) && Objects.equals(shuffleReadBytes, that.shuffleReadBytes) && Objects.equals(shuffleWriteBytes, that.shuffleWriteBytes) && Objects.equals(inputBytes, that.inputBytes) && Objects.equals(outputBytes, that.outputBytes) && Objects.equals(memorySpillBytes, that.memorySpillBytes) && Objects.equals(diskSpillBytes, that.diskSpillBytes) && Objects.equals(gcTimeMs, that.gcTimeMs) && Objects.equals(deserializeMs, that.deserializeMs); }
    @Override public int hashCode() { return Objects.hash(durationMs, shuffleReadBytes, shuffleWriteBytes, inputBytes, outputBytes, memorySpillBytes, diskSpillBytes, gcTimeMs, deserializeMs); }
}
