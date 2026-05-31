package io.sparkadvisor.monitor.rule;

import io.sparkadvisor.core.util.ValueObjects;

public final class QueueRuleThresholds {
    private final int minAnalyzedQueries; private final double commonBottleneckPct,mixedPartitionPct,highUtilization,lowUtilization,contentionLimitedPct,lowCpuEfficiency;
    public QueueRuleThresholds(int minAnalyzedQueries,double commonBottleneckPct,double mixedPartitionPct,double highUtilization,double lowUtilization,double contentionLimitedPct){this(minAnalyzedQueries,commonBottleneckPct,mixedPartitionPct,highUtilization,lowUtilization,contentionLimitedPct,0.35);}
    public QueueRuleThresholds(int minAnalyzedQueries,double commonBottleneckPct,double mixedPartitionPct,double highUtilization,double lowUtilization,double contentionLimitedPct,double lowCpuEfficiency){this.minAnalyzedQueries=minAnalyzedQueries;this.commonBottleneckPct=commonBottleneckPct;this.mixedPartitionPct=mixedPartitionPct;this.highUtilization=highUtilization;this.lowUtilization=lowUtilization;this.contentionLimitedPct=contentionLimitedPct;this.lowCpuEfficiency=lowCpuEfficiency;}
    public int minAnalyzedQueries(){return minAnalyzedQueries;} public double commonBottleneckPct(){return commonBottleneckPct;} public double mixedPartitionPct(){return mixedPartitionPct;} public double highUtilization(){return highUtilization;} public double lowUtilization(){return lowUtilization;} public double contentionLimitedPct(){return contentionLimitedPct;} public double lowCpuEfficiency(){return lowCpuEfficiency;}
    public static QueueRuleThresholds defaults(){ return new QueueRuleThresholds(5,0.30,0.15,0.85,0.35,0.25,0.35);}
    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);} }
