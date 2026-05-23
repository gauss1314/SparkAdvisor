package io.sparkadvisor.core.finding;

/**
 * A concrete tuning action attached to a finding.
 *
 * @param type        SQL_REWRITE or SPARK_CONF
 * @param action      the suggested change, e.g. "set spark.sql.adaptive.skewJoin.enabled=true"
 * @param rationale   why this helps, in plain language
 * @param expectedImpact qualitative or quantitative expected effect (may be null)
 */
public record Recommendation(
        Type type,
        String action,
        String rationale,
        String expectedImpact) {

    public enum Type {
        SQL_REWRITE,
        SPARK_CONF
    }

    public static Recommendation conf(String action, String rationale, String expectedImpact) {
        return new Recommendation(Type.SPARK_CONF, action, rationale, expectedImpact);
    }

    public static Recommendation sql(String action, String rationale, String expectedImpact) {
        return new Recommendation(Type.SQL_REWRITE, action, rationale, expectedImpact);
    }
}
