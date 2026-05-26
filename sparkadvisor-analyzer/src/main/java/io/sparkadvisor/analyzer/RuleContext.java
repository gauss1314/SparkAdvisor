package io.sparkadvisor.analyzer;

import io.sparkadvisor.core.analyze.SqlAnalysis;

public final class RuleContext {
    private final SqlAnalysis sql; private final RuleThresholds thresholds; private final AqeContext aqe;
    public RuleContext(SqlAnalysis sql, RuleThresholds thresholds, AqeContext aqe){this.sql=sql;this.thresholds=thresholds;this.aqe=aqe;}
    public SqlAnalysis sql(){return sql;} public RuleThresholds thresholds(){return thresholds;} public AqeContext aqe(){return aqe;}
}
