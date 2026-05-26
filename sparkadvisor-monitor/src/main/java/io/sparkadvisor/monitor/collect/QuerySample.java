package io.sparkadvisor.monitor.collect;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.util.ValueObjects;
import io.sparkadvisor.predictor.PredictionService;

import java.util.List;

public final class QuerySample {
    private final long executionId,startTime,endTime,durationMs,shuffleReadBytes,shuffleWriteBytes,spillBytes; private final String statementId,description; private final boolean running,failed; private final int stageCount; private final double maxSkewRatio,maxGcRatio,coreUtilization; private final SqlAnalysis sqlAnalysis; private final List<Finding> findings; private final PredictionService.Predictions predictions;
    public QuerySample(long executionId,String statementId,String description,long startTime,long endTime,boolean running,boolean failed,long durationMs,int stageCount,long shuffleReadBytes,long shuffleWriteBytes,long spillBytes,double maxSkewRatio,double maxGcRatio,double coreUtilization,SqlAnalysis sqlAnalysis,List<Finding> findings,PredictionService.Predictions predictions){this.executionId=executionId;this.statementId=statementId;this.description=description;this.startTime=startTime;this.endTime=endTime;this.running=running;this.failed=failed;this.durationMs=durationMs;this.stageCount=stageCount;this.shuffleReadBytes=shuffleReadBytes;this.shuffleWriteBytes=shuffleWriteBytes;this.spillBytes=spillBytes;this.maxSkewRatio=maxSkewRatio;this.maxGcRatio=maxGcRatio;this.coreUtilization=coreUtilization;this.sqlAnalysis=sqlAnalysis;this.findings=findings;this.predictions=predictions;}
    public long executionId(){return executionId;} public String statementId(){return statementId;} public String description(){return description;} public long startTime(){return startTime;} public long endTime(){return endTime;} public boolean running(){return running;} public boolean failed(){return failed;} public long durationMs(){return durationMs;} public int stageCount(){return stageCount;} public long shuffleReadBytes(){return shuffleReadBytes;} public long shuffleWriteBytes(){return shuffleWriteBytes;} public long spillBytes(){return spillBytes;} public double maxSkewRatio(){return maxSkewRatio;} public double maxGcRatio(){return maxGcRatio;} public double coreUtilization(){return coreUtilization;} public SqlAnalysis sqlAnalysis(){return sqlAnalysis;} public List<Finding> findings(){return findings;} public PredictionService.Predictions predictions(){return predictions;}
    public boolean deepAnalyzed(){ return findings != null && predictions != null; }
    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}
}
