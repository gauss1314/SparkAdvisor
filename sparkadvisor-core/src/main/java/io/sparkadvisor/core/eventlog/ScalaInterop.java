package io.sparkadvisor.core.eventlog;

import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Central place for all Scala &lt;-&gt; Java conversions of values returned by Spark events.
 * Keeping this isolated means the rest of {@code core} (and all of analyzer/predictor)
 * never imports Scala types directly.
 *
 * <p>All methods are defensive: nulls / empty Options become Java defaults rather than
 * throwing, because event logs can be truncated.
 *
 * <p>Several conversions touch version-sensitive shapes; those are marked
 * {@code // VERIFY@3.5.1} and should be confirmed on first real compile.
 */
final class ScalaInterop {

    private ScalaInterop() {}

    /**
     * Extract "Spark Properties" from an EnvironmentUpdate into a Java map.
     *
     * <p>{@code environmentDetails()} is a {@code scala.collection.Map[String, Seq[(String,String)]]}.
     * We read the "Spark Properties" entry. Implemented with the scala-java interop converters.
     */
    static Map<String, String> sparkProperties(SparkListenerEnvironmentUpdate e) {
        Map<String, String> out = new LinkedHashMap<>();
        // VERIFY@3.5.1: key name is "Spark Properties" in JsonProtocol.
        scala.collection.immutable.Map<String, scala.collection.Seq<scala.Tuple2<String, String>>> details =
                e.environmentDetails();
        scala.Option<scala.collection.Seq<scala.Tuple2<String, String>>> opt =
                details.get("Spark Properties");
        if (opt.isDefined()) {
            scala.collection.Seq<scala.Tuple2<String, String>> seq = opt.get();
            List<scala.Tuple2<String, String>> javaList =
                    scala.jdk.javaapi.CollectionConverters.asJava(seq);
            for (scala.Tuple2<String, String> kv : javaList) {
                out.put(kv._1(), kv._2());
            }
        }
        return out;
    }

    /** Read spark.sql.execution.id from job properties; null when absent. */
    static Long sqlExecutionId(Properties props) {
        if (props == null) return null;
        String v = props.getProperty("spark.sql.execution.id");
        if (v == null) return null;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    /** Convert a Scala Seq of (boxed) ints to a Java List&lt;Integer&gt;. */
    static List<Integer> intSeq(scala.collection.Seq<Object> seq) {
        List<Integer> out = new ArrayList<>();
        if (seq == null) return out;
        for (Object o : scala.jdk.javaapi.CollectionConverters.asJava(seq)) {
            out.add(((Number) o).intValue());
        }
        return out;
    }

    /** Unwrap a {@code scala.Option[Object]} holding a Long/Int to a primitive (0 when empty). */
    static long optLong(scala.Option<Object> opt) {
        if (opt == null || opt.isEmpty()) return 0L;
        Object v = opt.get();
        return ((Number) v).longValue();
    }
}
