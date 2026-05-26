package io.sparkadvisor.analyzer;

import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.core.util.ValueObjects;

import java.util.Map;

public final class AqeContext {
    private final boolean aqeEnabled,coalesceEnabled,skewJoinEnabled; private final long advisoryPartitionSizeBytes; private final int staticShufflePartitions;
    public AqeContext(boolean aqeEnabled, boolean coalesceEnabled, boolean skewJoinEnabled, long advisoryPartitionSizeBytes, int staticShufflePartitions){this.aqeEnabled=aqeEnabled;this.coalesceEnabled=coalesceEnabled;this.skewJoinEnabled=skewJoinEnabled;this.advisoryPartitionSizeBytes=advisoryPartitionSizeBytes;this.staticShufflePartitions=staticShufflePartitions;}
    public boolean aqeEnabled(){return aqeEnabled;} public boolean coalesceEnabled(){return coalesceEnabled;} public boolean skewJoinEnabled(){return skewJoinEnabled;} public long advisoryPartitionSizeBytes(){return advisoryPartitionSizeBytes;} public int staticShufflePartitions(){return staticShufflePartitions;}
    public static AqeContext from(Map<String, String> conf) { boolean aqe = bool(conf, "spark.sql.adaptive.enabled", true); boolean coalesce = bool(conf, "spark.sql.adaptive.coalescePartitions.enabled", true); boolean skewJoin = bool(conf, "spark.sql.adaptive.skewJoin.enabled", true); long advisory = bytes(conf, "spark.sql.adaptive.advisoryPartitionSizeInBytes", 64L * 1024 * 1024); int staticParts = intVal(conf, "spark.sql.shuffle.partitions", 200); return new AqeContext(aqe, coalesce, skewJoin, advisory, staticParts);}    
    private static boolean bool(Map<String, String> c, String k, boolean dflt) {String v = c.get(k); return v == null ? dflt : Boolean.parseBoolean(v.trim());}
    private static int intVal(Map<String, String> c, String k, int dflt) {String v = c.get(k); if (v == null) return dflt; try { return Integer.parseInt(v.trim()); } catch (NumberFormatException e) { return dflt; }}
    private static long bytes(Map<String, String> c, String k, long dflt) { String v = c.get(k); if (Strings.isBlank(v)) return dflt; v = v.trim().toLowerCase(); try { long mult = 1; char last = v.charAt(v.length() - 1); if (!Character.isDigit(last)) { if (last=='k') mult=1024L; else if (last=='m') mult=1024L*1024; else if (last=='g') mult=1024L*1024*1024; else if (last=='t') mult=1024L*1024*1024*1024; v = v.substring(0, v.length() - 1);} return Long.parseLong(v.trim()) * mult; } catch (RuntimeException e) { return dflt; } }
    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}
}
