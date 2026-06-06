package io.sparkadvisor.monitor.aggregate;

import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.predict.Confidence;
import io.sparkadvisor.core.util.Java8Collections;
import io.sparkadvisor.core.util.ValueObjects;
import io.sparkadvisor.report.model.AnalysisResult;

import java.util.List;

public final class QueueAnalysisResult {
    private final QueueSummary summary;
    private final List<HourBucketStat> timeline;
    private final List<BottleneckCluster> bottlenecks;
    private final UtilizationSeries utilization;
    private final ResourceMetrics resources;
    private final ContentionReport contention;
    private final List<SlowQueryRef> topSlowQueries;
    private final List<SlowQueryRef> sampledQueries;
    private final List<TemplateStat> templateStats;
    private final List<QueueRecommendation> globalRecommendations;
    private final AnalysisResult.AiAdvice aiAdvice;
    private final Meta meta;

    public QueueAnalysisResult(QueueSummary summary, List<HourBucketStat> timeline,
                               List<BottleneckCluster> bottlenecks, UtilizationSeries utilization,
                               ResourceMetrics resources, ContentionReport contention,
                               List<SlowQueryRef> topSlowQueries,
                               List<QueueRecommendation> globalRecommendations,
                               AnalysisResult.AiAdvice aiAdvice, Meta meta) {
        this(summary, timeline, bottlenecks, utilization, resources, contention, topSlowQueries,
                Java8Collections.<SlowQueryRef>listOf(), Java8Collections.<TemplateStat>listOf(),
                globalRecommendations, aiAdvice, meta);
    }

    public QueueAnalysisResult(QueueSummary summary, List<HourBucketStat> timeline,
                               List<BottleneckCluster> bottlenecks, UtilizationSeries utilization,
                               ResourceMetrics resources, ContentionReport contention,
                               List<SlowQueryRef> topSlowQueries, List<SlowQueryRef> sampledQueries,
                               List<QueueRecommendation> globalRecommendations,
                               AnalysisResult.AiAdvice aiAdvice, Meta meta) {
        this(summary, timeline, bottlenecks, utilization, resources, contention, topSlowQueries,
                sampledQueries, Java8Collections.<TemplateStat>listOf(), globalRecommendations, aiAdvice, meta);
    }

    public QueueAnalysisResult(QueueSummary summary, List<HourBucketStat> timeline,
                               List<BottleneckCluster> bottlenecks, UtilizationSeries utilization,
                               ResourceMetrics resources, ContentionReport contention,
                               List<SlowQueryRef> topSlowQueries, List<SlowQueryRef> sampledQueries,
                               List<TemplateStat> templateStats, List<QueueRecommendation> globalRecommendations,
                               AnalysisResult.AiAdvice aiAdvice, Meta meta) {
        this.summary = summary;
        this.timeline = Java8Collections.listCopy(timeline);
        this.bottlenecks = Java8Collections.listCopy(bottlenecks);
        this.utilization = utilization;
        this.resources = resources;
        this.contention = contention;
        this.topSlowQueries = Java8Collections.listCopy(topSlowQueries);
        this.sampledQueries = Java8Collections.listCopy(sampledQueries);
        this.templateStats = Java8Collections.listCopy(templateStats);
        this.globalRecommendations = Java8Collections.listCopy(globalRecommendations);
        this.aiAdvice = aiAdvice;
        this.meta = meta;
    }

    public QueueSummary summary(){return summary;} public List<HourBucketStat> timeline(){return timeline;}
    public List<BottleneckCluster> bottlenecks(){return bottlenecks;} public UtilizationSeries utilization(){return utilization;}
    public ResourceMetrics resources(){return resources;} public ContentionReport contention(){return contention;}
    public List<SlowQueryRef> topSlowQueries(){return topSlowQueries;} public List<SlowQueryRef> sampledQueries(){return sampledQueries;}
    public List<TemplateStat> templateStats(){return templateStats;}
    public List<QueueRecommendation> globalRecommendations(){return globalRecommendations;}
    public AnalysisResult.AiAdvice aiAdvice(){return aiAdvice;} public Meta meta(){return meta;}
    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}

    public QueueAnalysisResult withRecommendations(List<QueueRecommendation> recommendations) {
        return new QueueAnalysisResult(summary, timeline, bottlenecks, utilization, resources, contention,
                topSlowQueries, sampledQueries, templateStats, recommendations, aiAdvice, meta);
    }

    public QueueAnalysisResult withAiAdvice(AnalysisResult.AiAdvice advice) {
        return new QueueAnalysisResult(summary, timeline, bottlenecks, utilization, resources, contention,
                topSlowQueries, sampledQueries, templateStats, globalRecommendations, advice, meta);
    }

    public static final class QueueSummary {
        private final String appId, appName; private final long windowStart, windowEnd;
        private final int totalQueries, completedQueries, runningQueries, failedQueries, fixedExecutorCores;
        public QueueSummary(String appId, String appName, long windowStart, long windowEnd, int totalQueries,
                            int completedQueries, int runningQueries, int failedQueries, int fixedExecutorCores) {
            this.appId=appId; this.appName=appName; this.windowStart=windowStart; this.windowEnd=windowEnd;
            this.totalQueries=totalQueries; this.completedQueries=completedQueries; this.runningQueries=runningQueries;
            this.failedQueries=failedQueries; this.fixedExecutorCores=fixedExecutorCores;
        }
        public String appId(){return appId;} public String appName(){return appName;} public long windowStart(){return windowStart;}
        public long windowEnd(){return windowEnd;} public int totalQueries(){return totalQueries;} public int completedQueries(){return completedQueries;}
        public int runningQueries(){return runningQueries;} public int failedQueries(){return failedQueries;} public int fixedExecutorCores(){return fixedExecutorCores;}
        @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
        @Override public int hashCode(){return ValueObjects.hashFields(this);}
        @Override public String toString(){return ValueObjects.toString(this);}
    }

    public static final class HourBucketStat {
        private final long bucketStart,bucketEnd,p50Ms,p95Ms,p99Ms; private final int queryCount;
        private final double avgUtilization,cpuEfficiency,fetchWaitRatio,gcRatio,failedAttemptRatio,speculativeAttemptRatio;
        public HourBucketStat(long bucketStart,long bucketEnd,int queryCount,long p50Ms,long p95Ms,long p99Ms,double avgUtilization){this(bucketStart,bucketEnd,queryCount,p50Ms,p95Ms,p99Ms,avgUtilization,0.0,0.0,0.0,0.0,0.0);}
        public HourBucketStat(long bucketStart,long bucketEnd,int queryCount,long p50Ms,long p95Ms,long p99Ms,double avgUtilization,double cpuEfficiency,double fetchWaitRatio,double gcRatio,double failedAttemptRatio,double speculativeAttemptRatio){this.bucketStart=bucketStart;this.bucketEnd=bucketEnd;this.queryCount=queryCount;this.p50Ms=p50Ms;this.p95Ms=p95Ms;this.p99Ms=p99Ms;this.avgUtilization=avgUtilization;this.cpuEfficiency=cpuEfficiency;this.fetchWaitRatio=fetchWaitRatio;this.gcRatio=gcRatio;this.failedAttemptRatio=failedAttemptRatio;this.speculativeAttemptRatio=speculativeAttemptRatio;}
        public long bucketStart(){return bucketStart;} public long bucketEnd(){return bucketEnd;} public int queryCount(){return queryCount;} public long p50Ms(){return p50Ms;} public long p95Ms(){return p95Ms;} public long p99Ms(){return p99Ms;} public double avgUtilization(){return avgUtilization;} public double slotOccupancy(){return avgUtilization;} public double cpuEfficiency(){return cpuEfficiency;} public double fetchWaitRatio(){return fetchWaitRatio;} public double gcRatio(){return gcRatio;} public double failedAttemptRatio(){return failedAttemptRatio;} public double speculativeAttemptRatio(){return speculativeAttemptRatio;}
        @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
        @Override public int hashCode(){return ValueObjects.hashFields(this);}
        @Override public String toString(){return ValueObjects.toString(this);}
    }

    public static final class BottleneckCluster { private final String ruleId,category,scope; private final int affectedQueries; private final double affectedPct,sampleCoveragePct;
        public BottleneckCluster(String ruleId,String category,int affectedQueries,double affectedPct){this(ruleId,category,affectedQueries,affectedPct,"DEEP_SAMPLE",affectedPct);}
        public BottleneckCluster(String ruleId,String category,int affectedQueries,double affectedPct,String scope,double sampleCoveragePct){this.ruleId=ruleId;this.category=category;this.affectedQueries=affectedQueries;this.affectedPct=affectedPct;this.scope=scope;this.sampleCoveragePct=sampleCoveragePct;}
        public String ruleId(){return ruleId;} public String category(){return category;} public int affectedQueries(){return affectedQueries;} public double affectedPct(){return affectedPct;} public String scope(){return scope;} public double sampleCoveragePct(){return sampleCoveragePct;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);}}

    public static final class UtilizationSeries { private final List<Point> points; private final double avgUtilization,peakUtilization;
        public UtilizationSeries(List<Point> points,double avgUtilization,double peakUtilization){this.points=Java8Collections.listCopy(points);this.avgUtilization=avgUtilization;this.peakUtilization=peakUtilization;}
        public List<Point> points(){return points;} public double avgUtilization(){return avgUtilization;} public double peakUtilization(){return peakUtilization;}
        @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);}
        public static final class Point { private final long bucketStart,bucketEnd; private final double avgUtilization; public Point(long bucketStart,long bucketEnd,double avgUtilization){this.bucketStart=bucketStart;this.bucketEnd=bucketEnd;this.avgUtilization=avgUtilization;} public long bucketStart(){return bucketStart;} public long bucketEnd(){return bucketEnd;} public double avgUtilization(){return avgUtilization;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);} }
    }

    public static final class ResourceMetrics { private final long totalSpillBytes; private final double avgMaxGcRatio,p95MaxGcRatio,maxGcRatio,avgSlotOccupancy,avgCpuEfficiency,avgFetchWaitRatio,avgGcRatio,failedAttemptRatio,speculativeAttemptRatio;
        public ResourceMetrics(long totalSpillBytes,double avgMaxGcRatio,double p95MaxGcRatio,double maxGcRatio){this(totalSpillBytes,avgMaxGcRatio,p95MaxGcRatio,maxGcRatio,0.0,0.0,0.0,avgMaxGcRatio,0.0,0.0);}
        public ResourceMetrics(long totalSpillBytes,double avgMaxGcRatio,double p95MaxGcRatio,double maxGcRatio,double avgSlotOccupancy,double avgCpuEfficiency,double avgFetchWaitRatio,double avgGcRatio,double failedAttemptRatio,double speculativeAttemptRatio){this.totalSpillBytes=totalSpillBytes;this.avgMaxGcRatio=avgMaxGcRatio;this.p95MaxGcRatio=p95MaxGcRatio;this.maxGcRatio=maxGcRatio;this.avgSlotOccupancy=avgSlotOccupancy;this.avgCpuEfficiency=avgCpuEfficiency;this.avgFetchWaitRatio=avgFetchWaitRatio;this.avgGcRatio=avgGcRatio;this.failedAttemptRatio=failedAttemptRatio;this.speculativeAttemptRatio=speculativeAttemptRatio;}
        public long totalSpillBytes(){return totalSpillBytes;} public double avgMaxGcRatio(){return avgMaxGcRatio;} public double p95MaxGcRatio(){return p95MaxGcRatio;} public double maxGcRatio(){return maxGcRatio;} public double avgSlotOccupancy(){return avgSlotOccupancy;} public double avgCpuEfficiency(){return avgCpuEfficiency;} public double avgFetchWaitRatio(){return avgFetchWaitRatio;} public double avgGcRatio(){return avgGcRatio;} public double failedAttemptRatio(){return failedAttemptRatio;} public double speculativeAttemptRatio(){return speculativeAttemptRatio;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);}}

    public static final class ContentionReport { private final double contentionLimitedPct,inefficientBusyPct; private final List<Window> hotspots,starvationWindows; private final List<SlowQueryRef> topResourceHogs;
        public ContentionReport(double contentionLimitedPct,List<Window> hotspots,List<SlowQueryRef> topResourceHogs){this(contentionLimitedPct,0.0,hotspots,Java8Collections.<Window>listOf(),topResourceHogs);}
        public ContentionReport(double contentionLimitedPct,double inefficientBusyPct,List<Window> hotspots,List<Window> starvationWindows,List<SlowQueryRef> topResourceHogs){this.contentionLimitedPct=contentionLimitedPct;this.inefficientBusyPct=inefficientBusyPct;this.hotspots=Java8Collections.listCopy(hotspots);this.starvationWindows=Java8Collections.listCopy(starvationWindows);this.topResourceHogs=Java8Collections.listCopy(topResourceHogs);}
        public double contentionLimitedPct(){return contentionLimitedPct;} public double inefficientBusyPct(){return inefficientBusyPct;} public List<Window> hotspots(){return hotspots;} public List<Window> starvationWindows(){return starvationWindows;} public List<SlowQueryRef> topResourceHogs(){return topResourceHogs;}
        @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);}
        public static final class Window { private final long startTime,endTime; private final double avgUtilization; public Window(long startTime,long endTime,double avgUtilization){this.startTime=startTime;this.endTime=endTime;this.avgUtilization=avgUtilization;} public long startTime(){return startTime;} public long endTime(){return endTime;} public double avgUtilization(){return avgUtilization;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);} }
    }

    public static final class SlowQueryRef { private final String statementId,templateHash,dominantBottleneck; private final long executionId,startTime,endTime,durationMs,ownCoreMs; private final boolean contentionLimited,deepAnalyzed;
        public SlowQueryRef(String statementId,long executionId,long startTime,long endTime,long durationMs,String dominantBottleneck,boolean contentionLimited,long ownCoreMs){this(statementId,null,executionId,startTime,endTime,durationMs,dominantBottleneck,contentionLimited,ownCoreMs,false);}
        public SlowQueryRef(String statementId,String templateHash,long executionId,long startTime,long endTime,long durationMs,String dominantBottleneck,boolean contentionLimited,long ownCoreMs,boolean deepAnalyzed){this.statementId=statementId;this.templateHash=templateHash;this.executionId=executionId;this.startTime=startTime;this.endTime=endTime;this.durationMs=durationMs;this.dominantBottleneck=dominantBottleneck;this.contentionLimited=contentionLimited;this.ownCoreMs=ownCoreMs;this.deepAnalyzed=deepAnalyzed;}
        public String statementId(){return statementId;} public String templateHash(){return templateHash;} public long executionId(){return executionId;} public long startTime(){return startTime;} public long endTime(){return endTime;} public long durationMs(){return durationMs;} public String dominantBottleneck(){return dominantBottleneck;} public boolean contentionLimited(){return contentionLimited;} public long ownCoreMs(){return ownCoreMs;} public boolean deepAnalyzed(){return deepAnalyzed;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);}}

    public static final class TemplateStat { private final String templateHash,exampleStatementId; private final int queryCount; private final long totalDurationMs,totalCoreMs,totalInputBytes,totalShuffleReadBytes;
        public TemplateStat(String templateHash,String exampleStatementId,int queryCount,long totalDurationMs,long totalCoreMs,long totalInputBytes,long totalShuffleReadBytes){this.templateHash=templateHash;this.exampleStatementId=exampleStatementId;this.queryCount=queryCount;this.totalDurationMs=totalDurationMs;this.totalCoreMs=totalCoreMs;this.totalInputBytes=totalInputBytes;this.totalShuffleReadBytes=totalShuffleReadBytes;}
        public String templateHash(){return templateHash;} public String exampleStatementId(){return exampleStatementId;} public int queryCount(){return queryCount;} public long totalDurationMs(){return totalDurationMs;} public long totalCoreMs(){return totalCoreMs;} public long totalInputBytes(){return totalInputBytes;} public long totalShuffleReadBytes(){return totalShuffleReadBytes;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);}}

    public static final class QueueRecommendation { private final String queueRuleId,evidence,expectedCoverage,caveats; private final Recommendation recommendation; private final Confidence confidence;
        public QueueRecommendation(String queueRuleId,Recommendation recommendation,String evidence,Confidence confidence,String expectedCoverage){this(queueRuleId,recommendation,evidence,confidence,expectedCoverage,"");}
        public QueueRecommendation(String queueRuleId,Recommendation recommendation,String evidence,Confidence confidence,String expectedCoverage,String caveats){this.queueRuleId=queueRuleId;this.recommendation=recommendation;this.evidence=evidence;this.confidence=confidence;this.expectedCoverage=expectedCoverage;this.caveats=caveats;}
        public String queueRuleId(){return queueRuleId;} public Recommendation recommendation(){return recommendation;} public String evidence(){return evidence;} public Confidence confidence(){return confidence;} public String expectedCoverage(){return expectedCoverage;} public String caveats(){return caveats;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);}}

    public static final class Meta { private final String sparkAdvisorVersion,generatedAt,sourcePath,assumptions,snapshotKey,samplingStrategy,redactionPolicy,degradedReason; private final boolean incomplete,runningSnapshot,incremental; private final int deepAnalyzedTopN,lightAnalyzedQueries,deepAnalyzedQueries; private final double deepCoveragePct;
        public Meta(String sparkAdvisorVersion,String generatedAt,boolean incomplete,boolean runningSnapshot,String sourcePath,int deepAnalyzedTopN,String assumptions){this(sparkAdvisorVersion,generatedAt,incomplete,runningSnapshot,false,sourcePath,null,deepAnalyzedTopN,0,deepAnalyzedTopN,0.0,"topN","DEFAULT","",assumptions);}
        public Meta(String sparkAdvisorVersion,String generatedAt,boolean incomplete,boolean runningSnapshot,boolean incremental,String sourcePath,String snapshotKey,int deepAnalyzedTopN,int lightAnalyzedQueries,int deepAnalyzedQueries,double deepCoveragePct,String samplingStrategy,String redactionPolicy,String degradedReason,String assumptions){this.sparkAdvisorVersion=sparkAdvisorVersion;this.generatedAt=generatedAt;this.incomplete=incomplete;this.runningSnapshot=runningSnapshot;this.incremental=incremental;this.sourcePath=sourcePath;this.snapshotKey=snapshotKey;this.deepAnalyzedTopN=deepAnalyzedTopN;this.lightAnalyzedQueries=lightAnalyzedQueries;this.deepAnalyzedQueries=deepAnalyzedQueries;this.deepCoveragePct=deepCoveragePct;this.samplingStrategy=samplingStrategy;this.redactionPolicy=redactionPolicy;this.degradedReason=degradedReason;this.assumptions=assumptions;}
        public String sparkAdvisorVersion(){return sparkAdvisorVersion;} public String generatedAt(){return generatedAt;} public boolean incomplete(){return incomplete;} public boolean runningSnapshot(){return runningSnapshot;} public boolean incremental(){return incremental;} public String sourcePath(){return sourcePath;} public String snapshotKey(){return snapshotKey;} public int deepAnalyzedTopN(){return deepAnalyzedTopN;} public int lightAnalyzedQueries(){return lightAnalyzedQueries;} public int deepAnalyzedQueries(){return deepAnalyzedQueries;} public double deepCoveragePct(){return deepCoveragePct;} public String samplingStrategy(){return samplingStrategy;} public String redactionPolicy(){return redactionPolicy;} public String degradedReason(){return degradedReason;} public String assumptions(){return assumptions;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);}}
}
