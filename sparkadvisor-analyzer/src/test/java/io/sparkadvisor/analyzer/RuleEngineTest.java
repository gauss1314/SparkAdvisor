package io.sparkadvisor.analyzer;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Severity;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleEngineTest {

    private final PerformanceAnalyzer analyzer = new PerformanceAnalyzer();

    private static StageAnalysis stage(int id, double skew, double shufSkew, long shR,
                                       long spill, double gc, long schedDelay, long wall,
                                       long maxTask, long medTask) {
        return new StageAnalysis(id, 10, wall, maxTask, medTask, 14000, skew, shufSkew,
                shR, 0, spill, gc, schedDelay, 0, 0);
    }

    private static SqlAnalysis sql(double util, List<StageAnalysis> stages) {
        return new SqlAnalysis(42L, "stmt", "/* stmt */ select 1", "",
                15000, 10000, 2000, 0.5, util, stages);
    }

    private static Map<String, String> conf(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    private static Finding byRule(List<Finding> fs, String id) {
        return fs.stream().filter(f -> f.ruleId().equals(id)).findFirst().orElse(null);
    }

    @Test
    void detectsCriticalSkew() {
        var skew = stage(1, 18.0, 0, 53_000_000L, 0, 0, 0, 9000, 9000, 500);
        var fs = analyzer.analyze(sql(0.8, java.util.Arrays.asList(skew)), conf("spark.sql.adaptive.enabled", "false"));
        var f = byRule(fs, "R1_DATA_SKEW");
        assertNotNull(f);
        assertEquals(Severity.CRITICAL, f.severity());
    }

    @Test
    void aqeOffSuggestsEnablingAqe() {
        var skew = stage(1, 18.0, 0, 53_000_000L, 0, 0, 0, 9000, 9000, 500);
        var fs = analyzer.analyze(sql(0.8, java.util.Arrays.asList(skew)), conf("spark.sql.adaptive.enabled", "false"));
        var f = byRule(fs, "R1_DATA_SKEW");
        assertTrue(f.recommendations().get(0).action().contains("adaptive.enabled=true"));
    }

    @Test
    void aqeOnDoesNotSuggestEnablingAqeAndOffersSaltOrFactorTune() {
        var skew = stage(1, 18.0, 0, 53_000_000L, 0, 0, 0, 9000, 9000, 500);
        var fs = analyzer.analyze(sql(0.8, java.util.Arrays.asList(skew)),
                conf("spark.sql.adaptive.enabled", "true", "spark.sql.adaptive.skewJoin.enabled", "true"));
        var f = byRule(fs, "R1_DATA_SKEW");
        assertTrue(f.recommendations().stream()
                .noneMatch(r -> r.action().contains("adaptive.enabled=true")));
        assertTrue(f.recommendations().stream()
                .anyMatch(r -> r.action().contains("skewedPartitionFactor") || r.action().contains("salt")));
    }

    @Test
    void spillUnderAqeTargetsAdvisorySize() {
        var spill = stage(2, 1.1, 0, 1_000_000_000L, 800_000_000L, 0, 0, 8000, 1000, 900);
        var fs = analyzer.analyze(sql(0.8, java.util.Arrays.asList(spill)),
                conf("spark.sql.adaptive.enabled", "true",
                        "spark.sql.adaptive.coalescePartitions.enabled", "true"));
        var f = byRule(fs, "R2_EXCESSIVE_SPILL");
        assertNotNull(f);
        assertTrue(f.recommendations().get(0).action().contains("advisoryPartitionSizeInBytes"));
    }

    @Test
    void detectsLowParallelismGcAndSchedulingDelay() {
        assertNotNull(byRule(
                analyzer.analyze(sql(0.15, java.util.Arrays.asList(stage(3, 1.2, 0, 0, 0, 0, 0, 5000, 1200, 1000))), conf()),
                "R3_LOW_PARALLELISM"));
        assertNotNull(byRule(
                analyzer.analyze(sql(0.8, java.util.Arrays.asList(stage(4, 1.07, 0, 0, 0, 0.22, 0, 3000, 700, 650))), conf()),
                "R6_GC_PRESSURE"));
        assertNotNull(byRule(
                analyzer.analyze(sql(0.8, java.util.Arrays.asList(stage(5, 1.05, 0, 0, 0, 0, 5000, 10000, 2000, 1900))), conf()),
                "R8_SCHEDULING_DELAY"));
    }

    @Test
    void sortsCriticalFirst() {
        var skew = stage(1, 18.0, 0, 53_000_000L, 0, 0, 0, 9000, 9000, 500);
        var gc = stage(9, 1.07, 0, 0, 0, 0.22, 0, 3000, 700, 650);
        var fs = analyzer.analyze(sql(0.8, java.util.Arrays.asList(skew, gc)), conf("spark.sql.adaptive.enabled", "false"));
        assertFalse(fs.isEmpty());
        assertEquals(Severity.CRITICAL, fs.get(0).severity());
    }

    @Test
    void cleanStageProducesNoFindings() {
        var clean = stage(7, 1.04, 1.1, 0, 0, 0.02, 0, 1000, 520, 500);
        assertTrue(analyzer.analyze(sql(0.85, java.util.Arrays.asList(clean)), conf()).isEmpty());
    }

    @Test
    void detectsOverParallelism() {
        // 5000 tasks, median 50ms -> over-parallel
        var st = new StageAnalysis(3, 5000, 8000, 80, 50, 250000, 1.6, 0, 0, 0,
                0, 0.0, 0, 0, 0);
        var f = byRule(analyzer.analyze(sql(0.8, java.util.Arrays.asList(st)), conf()), "R4_OVER_PARALLELISM");
        assertNotNull(f);
    }

    @Test
    void detectsSmallFiles() {
        // 3000 input tasks, ~100KB each -> small files
        var st = new StageAnalysis(4, 3000, 9000, 600, 500, 1500000, 1.2, 0, 0, 0,
                0, 0.0, 0, 300_000_000L, 100_000L);
        var f = byRule(analyzer.analyze(sql(0.8, java.util.Arrays.asList(st)), conf()), "R5_SMALL_FILES");
        assertNotNull(f);
    }

    @Test
    void detectsBroadcastOpportunityFromPlan() {
        var st = stage(5, 1.1, 0, 1000, 0, 0.0, 0, 5000, 600, 500);
        var sqlWithPlan = new SqlAnalysis(42L, "stmt", "select 1",
                "== Physical Plan ==\nSortMergeJoin [k]\n  Scan a\n  Scan b",
                15000, 10000, 2000, 0.5, 0.8, java.util.Arrays.asList(st));
        var fs = analyzer.analyze(sqlWithPlan, conf());
        assertNotNull(byRule(fs, "R7_BROADCAST_JOIN"));
    }

    @Test
    void noBroadcastFindingWhenPlanAlreadyBroadcasts() {
        var st = stage(5, 1.1, 0, 1000, 0, 0.0, 0, 5000, 600, 500);
        var sqlWithPlan = new SqlAnalysis(42L, "stmt", "select 1",
                "== Physical Plan ==\nBroadcastHashJoin [k]\n  Scan a\n  Scan b",
                15000, 10000, 2000, 0.5, 0.8, java.util.Arrays.asList(st));
        assertEquals(null, byRule(analyzer.analyze(sqlWithPlan, conf()), "R7_BROADCAST_JOIN"));
    }

    @Test
    void detectsShuffleFetchWait() {
        var st = new StageAnalysis(6, 50, 10000, 1200, 700, 100000, 1.7, 1.3,
                2_000_000_000L, 0, 0, 0.01, 0, 0, 0,
                30_000L, 1_800_000_000L, 0, 0);
        var f = byRule(analyzer.analyze(sql(0.8, java.util.Arrays.asList(st)), conf()),
                "R9_SHUFFLE_FETCH_WAIT");
        assertNotNull(f);
        assertEquals("shuffle", f.category());
    }

    @Test
    void detectsTaskRetries() {
        var st = new StageAnalysis(7, 100, 10000, 1200, 700, 100000, 1.7, 1.3,
                0, 0, 0, 0.01, 0, 0, 0,
                0L, 0L, 2, 3);
        var f = byRule(analyzer.analyze(sql(0.8, java.util.Arrays.asList(st)), conf()),
                "R10_TASK_RETRY");
        assertNotNull(f);
        assertEquals(Severity.WARN, f.severity());
    }

    @Test
    void attributesSpillToSortOrAggregatePlan() {
        var st = stage(8, 1.1, 0, 1_000_000_000L, 900_000_000L, 0.0, 0, 5000, 600, 500);
        var sqlWithPlan = new SqlAnalysis(42L, "stmt", "select 1",
                "== Physical Plan ==\nHashAggregate(keys=[k])\n+- Sort [k]\n   +- Exchange hashpartitioning(k)",
                15000, 10000, 2000, 0.5, 0.8, java.util.Arrays.asList(st));
        assertNotNull(byRule(analyzer.analyze(sqlWithPlan, conf()), "R11_SORT_AGG_SPILL"));
    }
}
