package io.sparkadvisor.predictor.shuffle;

import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.predict.Confidence;
import io.sparkadvisor.core.predict.ShufflePartitionPrediction;
import io.sparkadvisor.predictor.costmodel.ShuffleCostModel;

import java.util.ArrayList;
import java.util.List;

public final class ShufflePartitionPredictor {
    private static final double SKEW_LIMIT = 5.0;
    private static final double FIXED_OVERHEAD_FRAC = 0.2;

    public ShufflePartitionPrediction predict(StageAnalysis stage, int currentPartitions, int cores,
                                              long perTaskMemoryBudget, boolean aqeCoalesceOn) {
        String knob = aqeCoalesceOn
                ? "spark.sql.adaptive.advisoryPartitionSizeInBytes (shuffle.partitions is only the upper bound)"
                : "spark.sql.shuffle.partitions";

        if (stage.skewRatio() >= SKEW_LIMIT || stage.shuffleSkewRatio() >= SKEW_LIMIT) {
            return new ShufflePartitionPrediction(
                    stage.stageId(), currentPartitions, stage.wallClockMs(),
                    currentPartitions, stage.wallClockMs(),
                    ShufflePartitionPrediction.Direction.SKEW_LIMITED,
                    new ArrayList<ShufflePartitionPrediction.Point>(),
                    knob,
                    Confidence.MEDIUM,
                    new ArrayList<String>(java.util.Arrays.asList("Stage is skewed (max/median >= " + SKEW_LIMIT + ").")),
                    "Repartitioning rarely helps a skewed stage; address skew first (AQE skew-join / salting), then re-evaluate partition count.");
        }

        long B = Math.max(stage.shuffleReadBytes(), stage.shuffleWriteBytes());
        if (B <= 0 || currentPartitions <= 0) {
            return degenerate(stage, currentPartitions, knob);
        }

        double observedBytesPerTask = (double) B / (double) currentPartitions;
        double observedMsPerTask = stage.medianTaskMs() > 0 ? stage.medianTaskMs() : 1;
        double[] or = ShuffleCostModel.fitOR(observedBytesPerTask, observedMsPerTask, FIXED_OVERHEAD_FRAC);
        ShuffleCostModel model = new ShuffleCostModel(B, cores, or[0], or[1], perTaskMemoryBudget);

        List<Integer> candidates = candidates(currentPartitions, cores);
        List<ShufflePartitionPrediction.Point> curve = new ArrayList<ShufflePartitionPrediction.Point>();
        int bestP = currentPartitions;
        long bestMs = Long.MAX_VALUE;
        for (int p : candidates) {
            long est = model.estimateMs(p);
            curve.add(new ShufflePartitionPrediction.Point(p, est));
            if (est < bestMs) {
                bestMs = est;
                bestP = p;
            }
        }
        long estCurrent = model.estimateMs(currentPartitions);

        ShufflePartitionPrediction.Direction dir;
        if (bestP > currentPartitions) dir = ShufflePartitionPrediction.Direction.FASTER_IF_INCREASED;
        else if (bestP < currentPartitions) dir = ShufflePartitionPrediction.Direction.FASTER_IF_DECREASED;
        else dir = ShufflePartitionPrediction.Direction.ALREADY_OPTIMAL;

        return new ShufflePartitionPrediction(
                stage.stageId(), currentPartitions, estCurrent, bestP, bestMs, dir, curve, knob,
                Confidence.MEDIUM,
                new ArrayList<String>(java.util.Arrays.asList(
                        "Fixed-overhead share assumed at " + (int) (FIXED_OVERHEAD_FRAC * 100) + "% of task time.",
                        "Throughput fit from a single operating point (median task).",
                        "Per-task memory budget = " + perTaskMemoryBudget + " bytes.")),
                "If the stage is actually skewed or task time is dominated by fixed overhead, the optimum shifts; treat the recommended count as a starting point.");
    }

    private List<Integer> candidates(int current, int cores) {
        java.util.LinkedHashSet<Integer> set = new java.util.LinkedHashSet<Integer>();
        for (double f : new double[]{0.25, 0.5, 1.0, 2.0, 4.0}) {
            int v = (int) Math.round(current * f);
            if (v >= 1) set.add(v);
        }
        if (cores >= 1) { set.add(cores); set.add(cores * 2); set.add(cores * 4); }
        return new ArrayList<Integer>(set);
    }

    private ShufflePartitionPrediction degenerate(StageAnalysis stage, int current, String knob) {
        return new ShufflePartitionPrediction(
                stage.stageId(), current, stage.wallClockMs(), current, stage.wallClockMs(),
                ShufflePartitionPrediction.Direction.ALREADY_OPTIMAL,
                new ArrayList<ShufflePartitionPrediction.Point>(),
                knob,
                Confidence.LOW,
                new ArrayList<String>(java.util.Arrays.asList("Insufficient shuffle volume to model partition sizing.")),
                "No actionable shuffle in this stage.");
    }
}
