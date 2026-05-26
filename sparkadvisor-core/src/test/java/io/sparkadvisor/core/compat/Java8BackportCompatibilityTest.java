package io.sparkadvisor.core.compat;

import io.sparkadvisor.core.analyze.CoreTimeline;
import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.locate.SqlLocator;
import io.sparkadvisor.core.metrics.Distribution;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.Job;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.predict.ExecutorScalingPrediction;
import io.sparkadvisor.core.predict.ShufflePartitionPrediction;
import io.sparkadvisor.core.util.Strings;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Java8BackportCompatibilityTest {

    @Test
    void java8StringBlankHelperMatchesNewerJdkBlankSemantics() {
        assertTrue(Strings.isBlank("\u2003\t\n"));
    }

    @Test
    void modelObjectsKeepRecordLikeValueSemantics() {
        assertEquals(new Distribution(2, 1, 1, 2, 2, 2, 3, 4),
                new Distribution(2, 1, 1, 2, 2, 2, 3, 4));
        assertEquals(new Job(1, 10L, List.of(2, 3), 100L, 200L, false),
                new Job(1, 10L, List.of(2, 3), 100L, 200L, false));
        assertEquals(new CoreTimeline.Segment(1L, 2L, 4),
                new CoreTimeline.Segment(1L, 2L, 4));
        assertEquals(new ShufflePartitionPrediction.Point(64, 1000L),
                new ShufflePartitionPrediction.Point(64, 1000L));
        assertEquals(new ExecutorScalingPrediction.Point(8, 1000L),
                new ExecutorScalingPrediction.Point(8, 1000L));

        String text = new Job(1, 10L, List.of(2, 3), 100L, 200L, false).toString();
        assertTrue(text.startsWith("Job["));
        assertTrue(text.contains("jobId=1"));
    }

    @Test
    void Java8CollectionReplacementsKeepUnmodifiablePublicResults() {
        SqlExecution older = exec(1L, "stmt", 100L);
        SqlExecution slower = exec(2L, "stmt", 300L);
        ApplicationModel app = new ApplicationModel("app-1", "test", 0L, 0L, false,
                Map.of(), List.of(older, slower), List.of(), List.of(), List.of(), List.of());

        List<SqlExecution> located = new SqlLocator(app).locate("stmt");
        assertEquals(List.of(slower, older), located);
        assertThrows(UnsupportedOperationException.class, () -> located.add(older));

        StageAnalysis shortStage = stage(1, 100L);
        StageAnalysis longStage = stage(2, 500L);
        SqlAnalysis sql = new SqlAnalysis(10L, "stmt", "select 1", "", 500L,
                500L, 250L, 0.0, 1.0, List.of(shortStage, longStage));

        assertThrows(UnsupportedOperationException.class, () -> sql.stages().add(shortStage));
        List<StageAnalysis> sorted = sql.stagesByDurationDesc();
        assertEquals(List.of(longStage, shortStage), sorted);
        assertThrows(UnsupportedOperationException.class, () -> sorted.add(shortStage));
    }

    private static SqlExecution exec(long id, String statementId, long durationMs) {
        return new SqlExecution(id, statementId, "/* " + statementId + " */ select 1", "",
                1_000L, 1_000L + durationMs, false, List.of(id));
    }

    private static StageAnalysis stage(int stageId, long wallClockMs) {
        return new StageAnalysis(stageId, 1, wallClockMs, wallClockMs, wallClockMs,
                wallClockMs, 1.0, 1.0, 0L, 0L, 0L, 0.0, 0L, 0L, 0L);
    }
}
