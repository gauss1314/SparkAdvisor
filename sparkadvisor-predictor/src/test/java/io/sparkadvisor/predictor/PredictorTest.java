package io.sparkadvisor.predictor;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.predict.ExecutorScalingPrediction;
import io.sparkadvisor.core.predict.ShufflePartitionPrediction;
import io.sparkadvisor.predictor.costmodel.ShuffleCostModel;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PredictorTest {

    private final PredictionService svc = new PredictionService();

    private static StageAnalysis stage(int id, int tasks, long wall, long maxT, long medT,
                                       long totalT, double skew, double shufSkew, long shR) {
        return new StageAnalysis(id, tasks, wall, maxT, medT, totalT, skew, shufSkew, shR, 0, 0, 0.0, 0, 0, 0);
    }

    private static SqlAnalysis sql(double util, List<StageAnalysis> st) {
        return new SqlAnalysis(1, "s", "sql", "", 15000, 10000, 2000, 0.5, util, st);
    }

    private static Map<String, String> conf(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    @Test
    void skewedStageIsSkewLimited() {
        var skew = stage(1, 10, 9000, 9000, 500, 14000, 18.0, 50.0, 53_000_000L);
        var p = svc.predict(sql(0.8, java.util.Arrays.asList(skew)),
                conf("spark.executor.instances", "4", "spark.executor.cores", "2"));
        assertNotNull(p.shuffle());
        assertEquals(ShufflePartitionPrediction.Direction.SKEW_LIMITED, p.shuffle().direction());
        assertTrue(p.shuffle().reversalNote().toLowerCase().contains("skew"));
    }

    @Test
    void underPartitionedNonSkewedRecommendsMorePartitions() {
        var under = stage(2, 4, 40000, 11000, 10000, 40000, 1.1, 1.0, 8_000_000_000L);
        var p = svc.predict(sql(0.2, java.util.Arrays.asList(under)),
                conf("spark.executor.instances", "4", "spark.executor.cores", "2"));
        var sp = p.shuffle();
        assertNotNull(sp);
        assertTrue(sp.recommendedPartitions() >= sp.currentPartitions());
        assertEquals(ShufflePartitionPrediction.Direction.FASTER_IF_INCREASED, sp.direction());
    }

    @Test
    void executorScalingFlooredBySkewStraggler() {
        var skew = stage(1, 10, 9000, 9000, 500, 14000, 18.0, 50.0, 53_000_000L);
        var p = svc.predict(sql(0.8, java.util.Arrays.asList(skew)),
                conf("spark.executor.instances", "4", "spark.executor.cores", "2"));
        ExecutorScalingPrediction ep = p.executor();
        assertNotNull(ep);
        assertTrue(ep.curve().size() >= 3);
        // Adding cores can never beat the 9s longest task.
        assertTrue(ep.curve().stream().allMatch(pt -> pt.estMs() >= 9000));
        assertTrue(ep.kneeCores() >= 1);
    }

    @Test
    void costModelReducesTimeWithMorePartitionsWhenSpilling() {
        var model = new ShuffleCostModel(8_000_000_000L, 8, 100, 50_000.0, 256L * 1024 * 1024);
        assertTrue(model.estimateMs(100) < model.estimateMs(10));
    }

    @Test
    void everyPredictionCarriesConfidenceAndAssumptions() {
        var st = stage(2, 8, 8000, 1100, 1000, 8000, 1.1, 1.0, 4_000_000_000L);
        var p = svc.predict(sql(0.5, java.util.Arrays.asList(st)),
                conf("spark.executor.instances", "4", "spark.executor.cores", "2"));
        assertNotNull(p.shuffle().confidence());
        assertTrue(!p.shuffle().assumptions().isEmpty());
        assertNotNull(p.executor().confidence());
        assertTrue(!p.executor().assumptions().isEmpty());
    }
}
