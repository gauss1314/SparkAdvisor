package io.sparkadvisor.monitor.collect;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.predictor.PredictionService;

import java.util.List;

/**
 * One SQL execution sample in a long-running queue application.
 *
 * <p>All executions get lightweight metrics. The slowest top-N completed executions also carry
 * full rule findings and predictions so queue-level aggregation can identify recurring causes
 * without doing the heaviest work for every query.
 */
public record QuerySample(
        long executionId,
        String statementId,
        String description,
        long startTime,
        long endTime,
        boolean running,
        boolean failed,
        long durationMs,
        int stageCount,
        long shuffleReadBytes,
        long shuffleWriteBytes,
        long spillBytes,
        double maxSkewRatio,
        double maxGcRatio,
        double coreUtilization,
        SqlAnalysis sqlAnalysis,
        List<Finding> findings,
        PredictionService.Predictions predictions) {

    public boolean deepAnalyzed() {
        return findings != null && predictions != null;
    }
}
