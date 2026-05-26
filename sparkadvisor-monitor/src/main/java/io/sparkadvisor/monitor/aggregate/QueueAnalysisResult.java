package io.sparkadvisor.monitor.aggregate;

import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.predict.Confidence;
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
    private final List<QueueRecommendation> globalRecommendations;
    private final AnalysisResult.AiAdvice aiAdvice;
    private final Meta meta;

    public QueueAnalysisResult(QueueSummary summary, List<HourBucketStat> timeline, List<BottleneckCluster> bottlenecks,
                               UtilizationSeries utilization, ResourceMetrics resources, ContentionReport contention,
                               List<SlowQueryRef> topSlowQueries, List<QueueRecommendation> globalRecommendations,
                               AnalysisResult.AiAdvice aiAdvice, Meta meta) {
        this.summary = summary; this.timeline = timeline; this.bottlenecks = bottlenecks; this.utilization = utilization;
        this.resources = resources; this.contention = contention; this.topSlowQueries = topSlowQueries;
        this.globalRecommendations = globalRecommendations; this.aiAdvice = aiAdvice; this.meta = meta;
    }
    public QueueSummary summary(){return summary;} public List<HourBucketStat> timeline(){return timeline;}
    public List<BottleneckCluster> bottlenecks(){return bottlenecks;} public UtilizationSeries utilization(){return utilization;}
    public ResourceMetrics resources(){return resources;} public ContentionReport contention(){return contention;}
    public List<SlowQueryRef> topSlowQueries(){return topSlowQueries;} public List<QueueRecommendation> globalRecommendations(){return globalRecommendations;}
    public AnalysisResult.AiAdvice aiAdvice(){return aiAdvice;} public Meta meta(){return meta;}

    public QueueAnalysisResult withRecommendations(List<QueueRecommendation> recommendations) {
        return new QueueAnalysisResult(summary, timeline, bottlenecks, utilization, resources, contention, topSlowQueries, recommendations, aiAdvice, meta);
    }
    public QueueAnalysisResult withAiAdvice(AnalysisResult.AiAdvice advice) {
        return new QueueAnalysisResult(summary, timeline, bottlenecks, utilization, resources, contention, topSlowQueries, globalRecommendations, advice, meta);
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
    }

    public static final class HourBucketStat {
        private final long bucketStart,bucketEnd,p50Ms,p95Ms,p99Ms; private final int queryCount; private final double avgUtilization;
        public HourBucketStat(long bucketStart,long bucketEnd,int queryCount,long p50Ms,long p95Ms,long p99Ms,double avgUtilization){this.bucketStart=bucketStart;this.bucketEnd=bucketEnd;this.queryCount=queryCount;this.p50Ms=p50Ms;this.p95Ms=p95Ms;this.p99Ms=p99Ms;this.avgUtilization=avgUtilization;}
        public long bucketStart(){return bucketStart;} public long bucketEnd(){return bucketEnd;} public int queryCount(){return queryCount;} public long p50Ms(){return p50Ms;} public long p95Ms(){return p95Ms;} public long p99Ms(){return p99Ms;} public double avgUtilization(){return avgUtilization;}
    }
    public static final class BottleneckCluster { private final String ruleId,category; private final int affectedQueries; private final double affectedPct;
        public BottleneckCluster(String ruleId,String category,int affectedQueries,double affectedPct){this.ruleId=ruleId;this.category=category;this.affectedQueries=affectedQueries;this.affectedPct=affectedPct;}
        public String ruleId(){return ruleId;} public String category(){return category;} public int affectedQueries(){return affectedQueries;} public double affectedPct(){return affectedPct;}}
    public static final class UtilizationSeries { private final List<Point> points; private final double avgUtilization,peakUtilization;
        public UtilizationSeries(List<Point> points,double avgUtilization,double peakUtilization){this.points=points;this.avgUtilization=avgUtilization;this.peakUtilization=peakUtilization;}
        public List<Point> points(){return points;} public double avgUtilization(){return avgUtilization;} public double peakUtilization(){return peakUtilization;}
        public static final class Point { private final long bucketStart,bucketEnd; private final double avgUtilization; public Point(long bucketStart,long bucketEnd,double avgUtilization){this.bucketStart=bucketStart;this.bucketEnd=bucketEnd;this.avgUtilization=avgUtilization;} public long bucketStart(){return bucketStart;} public long bucketEnd(){return bucketEnd;} public double avgUtilization(){return avgUtilization;} }
    }
    public static final class ResourceMetrics { private final long totalSpillBytes; private final double avgMaxGcRatio,p95MaxGcRatio,maxGcRatio;
        public ResourceMetrics(long totalSpillBytes,double avgMaxGcRatio,double p95MaxGcRatio,double maxGcRatio){this.totalSpillBytes=totalSpillBytes;this.avgMaxGcRatio=avgMaxGcRatio;this.p95MaxGcRatio=p95MaxGcRatio;this.maxGcRatio=maxGcRatio;}
        public long totalSpillBytes(){return totalSpillBytes;} public double avgMaxGcRatio(){return avgMaxGcRatio;} public double p95MaxGcRatio(){return p95MaxGcRatio;} public double maxGcRatio(){return maxGcRatio;}}
    public static final class ContentionReport { private final double contentionLimitedPct; private final List<Window> hotspots; private final List<SlowQueryRef> topResourceHogs;
        public ContentionReport(double contentionLimitedPct,List<Window> hotspots,List<SlowQueryRef> topResourceHogs){this.contentionLimitedPct=contentionLimitedPct;this.hotspots=hotspots;this.topResourceHogs=topResourceHogs;}
        public double contentionLimitedPct(){return contentionLimitedPct;} public List<Window> hotspots(){return hotspots;} public List<SlowQueryRef> topResourceHogs(){return topResourceHogs;}
        public static final class Window { private final long startTime,endTime; private final double avgUtilization; public Window(long startTime,long endTime,double avgUtilization){this.startTime=startTime;this.endTime=endTime;this.avgUtilization=avgUtilization;} public long startTime(){return startTime;} public long endTime(){return endTime;} public double avgUtilization(){return avgUtilization;} }
    }
    public static final class SlowQueryRef { private final String statementId,dominantBottleneck; private final long executionId,startTime,endTime,durationMs,ownCoreMs; private final boolean contentionLimited;
        public SlowQueryRef(String statementId,long executionId,long startTime,long endTime,long durationMs,String dominantBottleneck,boolean contentionLimited,long ownCoreMs){this.statementId=statementId;this.executionId=executionId;this.startTime=startTime;this.endTime=endTime;this.durationMs=durationMs;this.dominantBottleneck=dominantBottleneck;this.contentionLimited=contentionLimited;this.ownCoreMs=ownCoreMs;}
        public String statementId(){return statementId;} public long executionId(){return executionId;} public long startTime(){return startTime;} public long endTime(){return endTime;} public long durationMs(){return durationMs;} public String dominantBottleneck(){return dominantBottleneck;} public boolean contentionLimited(){return contentionLimited;} public long ownCoreMs(){return ownCoreMs;}}
    public static final class QueueRecommendation { private final String queueRuleId,evidence,expectedCoverage; private final Recommendation recommendation; private final Confidence confidence;
        public QueueRecommendation(String queueRuleId,Recommendation recommendation,String evidence,Confidence confidence,String expectedCoverage){this.queueRuleId=queueRuleId;this.recommendation=recommendation;this.evidence=evidence;this.confidence=confidence;this.expectedCoverage=expectedCoverage;}
        public String queueRuleId(){return queueRuleId;} public Recommendation recommendation(){return recommendation;} public String evidence(){return evidence;} public Confidence confidence(){return confidence;} public String expectedCoverage(){return expectedCoverage;}}
    public static final class Meta { private final String sparkAdvisorVersion,generatedAt,sourcePath,assumptions; private final boolean incomplete,runningSnapshot; private final int deepAnalyzedTopN;
        public Meta(String sparkAdvisorVersion,String generatedAt,boolean incomplete,boolean runningSnapshot,String sourcePath,int deepAnalyzedTopN,String assumptions){this.sparkAdvisorVersion=sparkAdvisorVersion;this.generatedAt=generatedAt;this.incomplete=incomplete;this.runningSnapshot=runningSnapshot;this.sourcePath=sourcePath;this.deepAnalyzedTopN=deepAnalyzedTopN;this.assumptions=assumptions;}
        public String sparkAdvisorVersion(){return sparkAdvisorVersion;} public String generatedAt(){return generatedAt;} public boolean incomplete(){return incomplete;} public boolean runningSnapshot(){return runningSnapshot;} public String sourcePath(){return sourcePath;} public int deepAnalyzedTopN(){return deepAnalyzedTopN;} public String assumptions(){return assumptions;}}
}
