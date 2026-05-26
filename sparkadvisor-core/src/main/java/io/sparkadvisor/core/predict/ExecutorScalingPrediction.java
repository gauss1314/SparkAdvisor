package io.sparkadvisor.core.predict;

import io.sparkadvisor.core.util.ValueObjects;

import java.util.List;
import java.util.Objects;

public final class ExecutorScalingPrediction {
    public static final class Point { private final int cores; private final long estMs; public Point(int cores,long estMs){this.cores=cores;this.estMs=estMs;} public int cores(){return cores;} public long estMs(){return estMs;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);} }
    private final int currentCores,kneeCores; private final long estCurrentMs; private final List<Point> curve; private final Confidence confidence; private final List<String> assumptions;
    public ExecutorScalingPrediction(int currentCores,long estCurrentMs,int kneeCores,List<Point> curve,Confidence confidence,List<String> assumptions){this.currentCores=currentCores;this.estCurrentMs=estCurrentMs;this.kneeCores=kneeCores;this.curve=curve;this.confidence=confidence;this.assumptions=assumptions;}
    public int currentCores(){return currentCores;} public long estCurrentMs(){return estCurrentMs;} public int kneeCores(){return kneeCores;} public List<Point> curve(){return curve;} public Confidence confidence(){return confidence;} public List<String> assumptions(){return assumptions;}
    @Override public boolean equals(Object o){ if(this==o)return true; if(!(o instanceof ExecutorScalingPrediction))return false; ExecutorScalingPrediction that=(ExecutorScalingPrediction)o; return currentCores==that.currentCores&&estCurrentMs==that.estCurrentMs&&kneeCores==that.kneeCores&&Objects.equals(curve,that.curve)&&confidence==that.confidence&&Objects.equals(assumptions,that.assumptions);} @Override public int hashCode(){return Objects.hash(currentCores,estCurrentMs,kneeCores,curve,confidence,assumptions);}
    @Override public String toString(){return ValueObjects.toString(this);} }
