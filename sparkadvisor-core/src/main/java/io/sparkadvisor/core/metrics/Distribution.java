package io.sparkadvisor.core.metrics;

import io.sparkadvisor.core.util.ValueObjects;

import java.util.Objects;

public final class Distribution {
    public static final Distribution EMPTY = new Distribution(0,0,0,0,0,0,0,0);
    private final long count,min,p25,median,p75,p90,max,sum;
    public Distribution(long count,long min,long p25,long median,long p75,long p90,long max,long sum){this.count=count;this.min=min;this.p25=p25;this.median=median;this.p75=p75;this.p90=p90;this.max=max;this.sum=sum;}
    public long count(){return count;} public long min(){return min;} public long p25(){return p25;} public long median(){return median;} public long p75(){return p75;} public long p90(){return p90;} public long max(){return max;} public long sum(){return sum;}
    public double mean(){return count==0?0.0:(double)sum/(double)count;} public double skewRatio(){return median==0?0.0:(double)max/(double)median;}
    @Override public boolean equals(Object o){if(this==o)return true; if(!(o instanceof Distribution))return false; Distribution d=(Distribution)o; return count==d.count&&min==d.min&&p25==d.p25&&median==d.median&&p75==d.p75&&p90==d.p90&&max==d.max&&sum==d.sum;}
    @Override public int hashCode(){return Objects.hash(count,min,p25,median,p75,p90,max,sum);}
    @Override public String toString(){return ValueObjects.toString(this);} }
