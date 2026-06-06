package io.sparkadvisor.monitor.rule;

import io.sparkadvisor.core.util.ValueObjects;

public final class QueueRuleThresholds {
    private final int minAnalyzedQueries;
    private final int highFrequencyTemplateMinQueries;
    private final double commonBottleneckPct;
    private final double mixedPartitionPct;
    private final double highUtilization;
    private final double lowUtilization;
    private final double contentionLimitedPct;
    private final double lowCpuEfficiency;
    private final double highAttemptRatio;
    private final double highFrequencyTemplatePct;
    private final double highFrequencyTemplateCostPct;
    private final long repeatedScanInputBytes;

    public QueueRuleThresholds(int minAnalyzedQueries, double commonBottleneckPct,
                               double mixedPartitionPct, double highUtilization,
                               double lowUtilization, double contentionLimitedPct) {
        this(minAnalyzedQueries, commonBottleneckPct, mixedPartitionPct, highUtilization,
                lowUtilization, contentionLimitedPct, 0.35);
    }

    public QueueRuleThresholds(int minAnalyzedQueries, double commonBottleneckPct,
                               double mixedPartitionPct, double highUtilization,
                               double lowUtilization, double contentionLimitedPct,
                               double lowCpuEfficiency) {
        this(minAnalyzedQueries, commonBottleneckPct, mixedPartitionPct, highUtilization,
                lowUtilization, contentionLimitedPct, lowCpuEfficiency,
                0.05, 3, 0.30, 0.30, 512L * 1024L * 1024L);
    }

    public QueueRuleThresholds(int minAnalyzedQueries, double commonBottleneckPct,
                               double mixedPartitionPct, double highUtilization,
                               double lowUtilization, double contentionLimitedPct,
                               double lowCpuEfficiency, double highAttemptRatio,
                               int highFrequencyTemplateMinQueries,
                               double highFrequencyTemplatePct,
                               double highFrequencyTemplateCostPct,
                               long repeatedScanInputBytes) {
        this.minAnalyzedQueries = minAnalyzedQueries;
        this.commonBottleneckPct = commonBottleneckPct;
        this.mixedPartitionPct = mixedPartitionPct;
        this.highUtilization = highUtilization;
        this.lowUtilization = lowUtilization;
        this.contentionLimitedPct = contentionLimitedPct;
        this.lowCpuEfficiency = lowCpuEfficiency;
        this.highAttemptRatio = highAttemptRatio;
        this.highFrequencyTemplateMinQueries = highFrequencyTemplateMinQueries;
        this.highFrequencyTemplatePct = highFrequencyTemplatePct;
        this.highFrequencyTemplateCostPct = highFrequencyTemplateCostPct;
        this.repeatedScanInputBytes = repeatedScanInputBytes;
    }

    public int minAnalyzedQueries(){return minAnalyzedQueries;}
    public int highFrequencyTemplateMinQueries(){return highFrequencyTemplateMinQueries;}
    public double commonBottleneckPct(){return commonBottleneckPct;}
    public double mixedPartitionPct(){return mixedPartitionPct;}
    public double highUtilization(){return highUtilization;}
    public double lowUtilization(){return lowUtilization;}
    public double contentionLimitedPct(){return contentionLimitedPct;}
    public double lowCpuEfficiency(){return lowCpuEfficiency;}
    public double highAttemptRatio(){return highAttemptRatio;}
    public double highFrequencyTemplatePct(){return highFrequencyTemplatePct;}
    public double highFrequencyTemplateCostPct(){return highFrequencyTemplateCostPct;}
    public long repeatedScanInputBytes(){return repeatedScanInputBytes;}

    public static QueueRuleThresholds defaults() {
        return new QueueRuleThresholds(5, 0.30, 0.15, 0.85, 0.35, 0.25,
                0.35, 0.05, 3, 0.30, 0.30, 512L * 1024L * 1024L);
    }

    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}
}
