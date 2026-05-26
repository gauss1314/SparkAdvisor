package io.sparkadvisor.analyzer;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.util.ValueObjects;

public final class RuleContext {
    private final SqlAnalysis sql; private final RuleThresholds thresholds; private final AqeContext aqe;
    public RuleContext(SqlAnalysis sql, RuleThresholds thresholds, AqeContext aqe){this.sql=sql;this.thresholds=thresholds;this.aqe=aqe;}
    public SqlAnalysis sql(){return sql;} public RuleThresholds thresholds(){return thresholds;} public AqeContext aqe(){return aqe;}
    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}
}
