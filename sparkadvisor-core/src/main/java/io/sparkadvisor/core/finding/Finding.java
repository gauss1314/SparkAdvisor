package io.sparkadvisor.core.finding;

import io.sparkadvisor.core.util.ValueObjects;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Finding {
    private final String ruleId,category,explanation,confidence,caveat,suppressionReason; private final Severity severity; private final Integer targetStageId; private final Map<String,String> evidence; private final List<Recommendation> recommendations; private final double score; private final boolean suppressed;
    public Finding(String ruleId,String category,Severity severity,Integer targetStageId,String explanation,Map<String,String> evidence,List<Recommendation> recommendations){this(ruleId,category,severity,targetStageId,explanation,evidence,recommendations,severityBase(severity),"HIGH","",false,"");}
    public Finding(String ruleId,String category,Severity severity,Integer targetStageId,String explanation,Map<String,String> evidence,List<Recommendation> recommendations,double score,String confidence,String caveat,boolean suppressed,String suppressionReason){this.ruleId=ruleId;this.category=category;this.severity=severity;this.targetStageId=targetStageId;this.explanation=explanation;this.evidence=evidence;this.recommendations=recommendations;this.score=score;this.confidence=confidence;this.caveat=caveat;this.suppressed=suppressed;this.suppressionReason=suppressionReason;}
    public String ruleId(){return ruleId;} public String category(){return category;} public Severity severity(){return severity;} public Integer targetStageId(){return targetStageId;} public String explanation(){return explanation;} public Map<String,String> evidence(){return evidence;} public List<Recommendation> recommendations(){return recommendations;} public double score(){return score;} public String confidence(){return confidence;} public String caveat(){return caveat;} public boolean suppressed(){return suppressed;} public String suppressionReason(){return suppressionReason;}
    public Finding withSuppression(String reason){return new Finding(ruleId,category,severity,targetStageId,explanation,evidence,recommendations,score,confidence,caveat,true,reason==null?"":reason);}
    public Finding withQuality(Severity adjustedSeverity,String adjustedConfidence,String adjustedCaveat){double adjustedScore=severityBase(severity)<=0.0?score:score*severityBase(adjustedSeverity)/severityBase(severity);return new Finding(ruleId,category,adjustedSeverity,targetStageId,explanation,evidence,recommendations,adjustedScore,adjustedConfidence,adjustedCaveat,suppressed,suppressionReason);}
    private static double severityBase(Severity severity){return severity==Severity.CRITICAL?100.0:severity==Severity.WARN?10.0:1.0;}
    @Override public boolean equals(Object o){if(this==o)return true; if(!(o instanceof Finding))return false; Finding f=(Finding)o; return Double.compare(f.score,score)==0&&suppressed==f.suppressed&&Objects.equals(ruleId,f.ruleId)&&Objects.equals(category,f.category)&&severity==f.severity&&Objects.equals(targetStageId,f.targetStageId)&&Objects.equals(explanation,f.explanation)&&Objects.equals(evidence,f.evidence)&&Objects.equals(recommendations,f.recommendations)&&Objects.equals(confidence,f.confidence)&&Objects.equals(caveat,f.caveat)&&Objects.equals(suppressionReason,f.suppressionReason);} @Override public int hashCode(){return Objects.hash(ruleId,category,severity,targetStageId,explanation,evidence,recommendations,score,confidence,caveat,suppressed,suppressionReason);}
    @Override public String toString(){return ValueObjects.toString(this);} }
