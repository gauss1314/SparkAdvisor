package io.sparkadvisor.analyzer;

import java.util.Map;

/**
 * AQE-relevant configuration, parsed from the application's {@code spark.*} conf.
 *
 * <p>Spark 3.5 enables AQE by default, which auto-coalesces shuffle partitions and
 * auto-splits skewed partitions at runtime. Rules MUST consult this so they don't, e.g.,
 * tell the user to "enable AQE" when it's already on, or to bump
 * {@code spark.sql.shuffle.partitions} when the effective knob is
 * {@code advisoryPartitionSizeInBytes}. See design doc §8.2.
 */
public record AqeContext(
        boolean aqeEnabled,
        boolean coalesceEnabled,
        boolean skewJoinEnabled,
        long advisoryPartitionSizeBytes,
        int staticShufflePartitions) {

    public static AqeContext from(Map<String, String> conf) {
        boolean aqe = bool(conf, "spark.sql.adaptive.enabled", true);            // 3.5 default: true
        boolean coalesce = bool(conf, "spark.sql.adaptive.coalescePartitions.enabled", true);
        boolean skewJoin = bool(conf, "spark.sql.adaptive.skewJoin.enabled", true);
        long advisory = bytes(conf, "spark.sql.adaptive.advisoryPartitionSizeInBytes", 64L * 1024 * 1024);
        int staticParts = intVal(conf, "spark.sql.shuffle.partitions", 200);
        return new AqeContext(aqe, coalesce, skewJoin, advisory, staticParts);
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

    /** Parse a byte size that may be plain bytes or have a k/m/g suffix (e.g. "64m"). */
    private static long bytes(Map<String, String> c, String k, long dflt) {
        String v = c.get(k);
        if (v == null || v.isBlank()) return dflt;
        v = v.trim().toLowerCase();
        try {
            long mult = 1;
            char last = v.charAt(v.length() - 1);
            if (!Character.isDigit(last)) {
                mult = switch (last) {
                    case 'k' -> 1024L;
                    case 'm' -> 1024L * 1024;
                    case 'g' -> 1024L * 1024 * 1024;
                    case 't' -> 1024L * 1024 * 1024 * 1024;
                    default -> 1L;
                };
                v = v.substring(0, v.length() - 1);
            }
            return Long.parseLong(v.trim()) * mult;
        } catch (RuntimeException e) {
            return dflt;
        }
    }
}
