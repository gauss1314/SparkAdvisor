package io.sparkadvisor.monitor.rule;

import io.sparkadvisor.analyzer.v2.Capability;
import io.sparkadvisor.analyzer.v2.MetricsContext;
import io.sparkadvisor.analyzer.v2.RuleScope;
import io.sparkadvisor.analyzer.v2.RuleThresholdsV2;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.ExecutorEvent;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.monitor.collect.QuerySample;

import java.util.List;
import java.util.Map;
import java.util.Collections;

/** Pre-aggregates currently available monitor evidence into rules.md Q metric keys. */
final class QueueMetricsContextAdapter {
    private QueueMetricsContextAdapter() {}

    static MetricsContext from(QueueAnalysisResult result,ApplicationModel app,List<QuerySample> samples,RuleThresholdsV2 thresholds){
        List<QuerySample> safeSamples=samples==null?Collections.<QuerySample>emptyList():samples;
        MetricsContext.Builder b=MetricsContext.builder(RuleScope.QUEUE).capability(Capability.QUEUE_TIMELINE)
                .partial(result.meta().incomplete())
                .number("queue_hotspot_count",result.contention().hotspots().size())
                .number("avg_utilization",result.utilization().avgUtilization())
                .number("contention_limited_pct",result.contention().contentionLimitedPct())
                .number("driver.gc_ratio",result.resources().avgGcRatio())
                .number("storm.fail_ratio",maxFailedRatio(result))
                .number("storm.failed_tasks",maxFailedTasks(result))
                .number("storm.stage_retries",safeSamples.stream().mapToInt(QuerySample::extraTaskAttempts).sum())
                .number("memory.pressure_events",countClusters(result,"S-07")+countClusters(result,"S-08")+countClusters(result,"S-09"))
                .number("small_files.affected_queries",countClusters(result,"S-05")+countClusters(result,"S-06"))
                .number("executor.removed_count",removedExecutors(app))
                .number("config.issue_count",app==null?0:configurationIssueCount(app.conf()))
                .number("idle_window_minutes",longestWindowMinutes(result,false,thresholds))
                .number("overload_window_minutes",longestWindowMinutes(result,true,thresholds));
        if(hasCluster(result,"S-05")||hasCluster(result,"S-06"))b.capability(Capability.PLAN_METRICS);
        int interrupted=(app!=null&&app.endTime()>0L)?result.summary().runningQueries():0;
        long tailMs=Math.round(thresholds.get("restart_win.tail_min")*60_000.0);int tail=0;for(QuerySample sample:safeSamples)if(app!=null&&app.endTime()>0L&&sample.startTime()>=app.endTime()-tailMs)tail++;
        b.number("restart.interrupted_queries",interrupted).number("restart.tail_submissions",tail);
        return b.build();
    }

    private static boolean hasCluster(QueueAnalysisResult r,String id){return countClusters(r,id)>0;}
    private static int countClusters(QueueAnalysisResult r,String id){for(QueueAnalysisResult.BottleneckCluster c:r.bottlenecks())if(id.equals(c.ruleId()))return c.affectedQueries();return 0;}
    private static int removedExecutors(ApplicationModel app){if(app==null)return 0;int n=0;for(ExecutorEvent event:app.executorEvents())if(!event.added())n++;return n;}
    private static int configurationIssueCount(Map<String,String> conf){int n=0;if(!"true".equalsIgnoreCase(conf.get("spark.sql.adaptive.enabled")))n++;if(!"true".equalsIgnoreCase(conf.get("spark.eventLog.rolling.enabled")))n++;if(!"true".equalsIgnoreCase(conf.get("spark.eventLog.logStageExecutorMetrics")))n++;if(!"FAIR".equalsIgnoreCase(conf.get("spark.scheduler.mode")))n++;return n;}
    private static double maxFailedRatio(QueueAnalysisResult r){double max=0;for(QueueAnalysisResult.HourBucketStat s:r.timeline())max=Math.max(max,s.failedAttemptRatio());return max;}
    private static int maxFailedTasks(QueueAnalysisResult r){int max=0;for(QueueAnalysisResult.HourBucketStat s:r.timeline())max=Math.max(max,(int)Math.round(s.failedAttemptRatio()*s.taskCount()));return max;}
    private static double longestWindowMinutes(QueueAnalysisResult r,boolean overload,RuleThresholdsV2 thresholds){long longest=0,current=0;double boundary=thresholds.get(overload?"capacity.overload_ratio":"capacity.idle_ratio");for(QueueAnalysisResult.HourBucketStat s:r.timeline()){boolean hit=overload?s.avgUtilization()>boundary:s.avgUtilization()<boundary;if(hit){current+=Math.max(0,s.bucketEnd()-s.bucketStart());longest=Math.max(longest,current);}else current=0;}return longest/60000.0;}
}
