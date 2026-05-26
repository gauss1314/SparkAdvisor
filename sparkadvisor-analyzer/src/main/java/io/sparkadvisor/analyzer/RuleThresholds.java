package io.sparkadvisor.analyzer;

import io.sparkadvisor.core.util.ValueObjects;

public final class RuleThresholds {
    private final double skewRatioWarn,skewRatioCritical,shuffleSkewWarn,spillRatioWarn,gcRatioWarn,coreUtilLow,schedulingDelayRatioWarn; private final long smallTaskMedianMs,smallInputPerTaskBytes; private final int overParallelMinTasks;
    public RuleThresholds(double skewRatioWarn,double skewRatioCritical,double shuffleSkewWarn,double spillRatioWarn,double gcRatioWarn,double coreUtilLow,long smallTaskMedianMs,int overParallelMinTasks,long smallInputPerTaskBytes,double schedulingDelayRatioWarn){this.skewRatioWarn=skewRatioWarn;this.skewRatioCritical=skewRatioCritical;this.shuffleSkewWarn=shuffleSkewWarn;this.spillRatioWarn=spillRatioWarn;this.gcRatioWarn=gcRatioWarn;this.coreUtilLow=coreUtilLow;this.smallTaskMedianMs=smallTaskMedianMs;this.overParallelMinTasks=overParallelMinTasks;this.smallInputPerTaskBytes=smallInputPerTaskBytes;this.schedulingDelayRatioWarn=schedulingDelayRatioWarn;}
    public double skewRatioWarn(){return skewRatioWarn;} public double skewRatioCritical(){return skewRatioCritical;} public double shuffleSkewWarn(){return shuffleSkewWarn;} public double spillRatioWarn(){return spillRatioWarn;} public double gcRatioWarn(){return gcRatioWarn;} public double coreUtilLow(){return coreUtilLow;} public long smallTaskMedianMs(){return smallTaskMedianMs;} public int overParallelMinTasks(){return overParallelMinTasks;} public long smallInputPerTaskBytes(){return smallInputPerTaskBytes;} public double schedulingDelayRatioWarn(){return schedulingDelayRatioWarn;}
    public static RuleThresholds defaults(){ return new RuleThresholds(5.0,10.0,5.0,0.5,0.10,0.40,200L,2000,4L*1024*1024,0.30);}
    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}
}
