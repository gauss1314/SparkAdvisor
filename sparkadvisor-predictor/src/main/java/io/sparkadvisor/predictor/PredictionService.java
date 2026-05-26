package io.sparkadvisor.predictor;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.predict.ExecutorScalingPrediction;
import io.sparkadvisor.core.predict.ShufflePartitionPrediction;
import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.core.util.ValueObjects;
import io.sparkadvisor.predictor.executor.ExecutorScalingPredictor;
import io.sparkadvisor.predictor.shuffle.ShufflePartitionPredictor;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

/**
 * Facade that produces both predictions for a SQL: the shuffle-partition prediction for the
 * dominant shuffle stage, and the executor-scaling curve for the whole SQL.
 */
public final class PredictionService {

    private final ShufflePartitionPredictor shufflePredictor = new ShufflePartitionPredictor();
    private final ExecutorScalingPredictor executorPredictor = new ExecutorScalingPredictor();

    public static final class Predictions {
        private final ShufflePartitionPrediction shuffle;
        private final ExecutorScalingPrediction executor;
        public Predictions(ShufflePartitionPrediction shuffle, ExecutorScalingPrediction executor){this.shuffle=shuffle;this.executor=executor;}
        public ShufflePartitionPrediction shuffle(){return shuffle;}
        public ExecutorScalingPrediction executor(){return executor;}
        @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
        @Override public int hashCode(){return ValueObjects.hashFields(this);}
        @Override public String toString(){return ValueObjects.toString(this);}
    }

    /**
     * @param sql   analyzed SQL
     * @param conf  application spark.* conf (for cores, memory budget, AQE flags)
     */
    public Predictions predict(SqlAnalysis sql, Map<String, String> conf) {
        int cores = readCores(conf);
        long perTaskMem = perTaskMemoryBudget(conf, cores);
        boolean aqeCoalesce = bool(conf, "spark.sql.adaptive.enabled", true)
                && bool(conf, "spark.sql.adaptive.coalescePartitions.enabled", true);

        ShufflePartitionPrediction shuffle = dominantShuffleStage(sql)
                .map(st -> shufflePredictor.predict(st, st.numTasks(), cores, perTaskMem, aqeCoalesce))
                .orElse(null);

        ExecutorScalingPrediction executor = executorPredictor.predict(sql, cores);
        return new Predictions(shuffle, executor);
    }

    /** The shuffle stage with the most shuffle bytes (read or write). */
    private Optional<StageAnalysis> dominantShuffleStage(SqlAnalysis sql) {
        return sql.stages().stream()
                .filter(s -> s.shuffleReadBytes() > 0 || s.shuffleWriteBytes() > 0)
                .max(Comparator.comparingLong(s -> Math.max(s.shuffleReadBytes(), s.shuffleWriteBytes())));
    }

    private int readCores(Map<String, String> conf) {
        int instances = intVal(conf, "spark.executor.instances", 0);
        int cores = intVal(conf, "spark.executor.cores", 1);
        int total = instances * cores;
        return total > 0 ? total : 1;
    }

    /**
     * Rough per-task memory budget: executor memory * a fraction split across cores.
     * Coarse; refine with spark.memory.fraction and actual peak metrics later (M2+ TODO).
     */
    private long perTaskMemoryBudget(Map<String, String> conf, int cores) {
        long execMem = memBytes(conf, "spark.executor.memory", 4L * 1024 * 1024 * 1024); // 4g default
        int coresPerExec = Math.max(1, intVal(conf, "spark.executor.cores", 1));
        // ~60% usable for execution+storage, divided across cores in the executor.
        return (long) (execMem * 0.6 / coresPerExec);
    }

    private static boolean bool(Map<String, String> c, String k, boolean dflt) {
        String v = c.get(k);
        return v == null ? dflt : Boolean.parseBoolean(v.trim());
    }

    private static int intVal(Map<String, String> c, String k, int dflt) {
        String v = c.get(k);
        if (v == null) return dflt;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return dflt;
        }
    }

    private static long memBytes(Map<String, String> c, String k, long dflt) {
        String v = c.get(k);
        if (Strings.isBlank(v)) return dflt;
        v = v.trim().toLowerCase();
        try {
            long mult = 1;
            char last = v.charAt(v.length() - 1);
            if (!Character.isDigit(last)) {
                if (last == 'k') mult = 1024L;
                else if (last == 'm') mult = 1024L * 1024;
                else if (last == 'g') mult = 1024L * 1024 * 1024;
                else if (last == 't') mult = 1024L * 1024 * 1024 * 1024;
                else mult = 1L;
                v = v.substring(0, v.length() - 1);
            }
            return Long.parseLong(v.trim()) * mult;
        } catch (RuntimeException e) {
            return dflt;
        }
    }
}
