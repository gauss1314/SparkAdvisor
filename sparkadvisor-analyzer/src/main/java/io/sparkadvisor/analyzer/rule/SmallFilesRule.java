package io.sparkadvisor.analyzer.rule;

import io.sparkadvisor.analyzer.RuleContext;
import io.sparkadvisor.analyzer.RuleThresholds;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * R5 — Small files. Triggers on an input (scan) stage where there are many tasks but each
 * reads only a tiny amount of source data — the classic small-files problem, where the file
 * count drives task count and overhead instead of data volume.
 */
public final class SmallFilesRule implements Rule {

    @Override
    public String id() {
        return "R5_SMALL_FILES";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        RuleThresholds t = ctx.thresholds();
        List<Finding> findings = new ArrayList<>();
        for (StageAnalysis st : ctx.sql().stages()) {
            // Only consider stages that actually read source input (not shuffle stages).
            boolean isInputStage = st.inputBytes() > 0;
            boolean manyTasks = st.numTasks() >= t.overParallelMinTasks();
            boolean tinyPerTask = st.medianInputBytesPerTask() > 0
                    && st.medianInputBytesPerTask() < t.smallInputPerTaskBytes();
            if (!(isInputStage && manyTasks && tinyPerTask)) {
                continue;
            }
            Map<String, String> evidence = new LinkedHashMap<>();
            evidence.put("numTasks", String.valueOf(st.numTasks()));
            evidence.put("medianInputBytesPerTask", String.valueOf(st.medianInputBytesPerTask()));
            evidence.put("totalInputBytes", String.valueOf(st.inputBytes()));

            String explanation = String.format(
                    "Stage %d reads source data across %d tasks but only ~%d bytes per task — "
                            + "a small-files pattern; file count, not data volume, drives the task count.",
                    st.stageId(), st.numTasks(), st.medianInputBytesPerTask());

            List<Recommendation> recs = new java.util.ArrayList<Recommendation>(java.util.Arrays.asList(
                    Recommendation.sql(
                            "compact the source into larger files (e.g. periodic OPTIMIZE/compaction, "
                                    + "or repartition on write)",
                            "Fewer, larger files reduce task count and scheduling overhead on every read.",
                            "Addresses the root cause for all future reads."),
                    Recommendation.conf(
                            "raise spark.sql.files.maxPartitionBytes / openCostInBytes to pack more "
                                    + "small files per task",
                            "Lets Spark combine more small files into each scan task.",
                            "Mitigates symptoms without rewriting data.")));

            findings.add(new Finding(id(), "small-files", Severity.WARN, st.stageId(),
                    explanation, evidence, recs));
        }
        return findings;
    }
}
