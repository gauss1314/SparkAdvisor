package io.sparkadvisor.core.finding;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Finding {
    private final String ruleId,category,explanation; private final Severity severity; private final Integer targetStageId; private final Map<String,String> evidence; private final List<Recommendation> recommendations;
    public Finding(String ruleId,String category,Severity severity,Integer targetStageId,String explanation,Map<String,String> evidence,List<Recommendation> recommendations){this.ruleId=ruleId;this.category=category;this.severity=severity;this.targetStageId=targetStageId;this.explanation=explanation;this.evidence=evidence;this.recommendations=recommendations;}
    public String ruleId(){return ruleId;} public String category(){return category;} public Severity severity(){return severity;} public Integer targetStageId(){return targetStageId;} public String explanation(){return explanation;} public Map<String,String> evidence(){return evidence;} public List<Recommendation> recommendations(){return recommendations;}
    @Override public boolean equals(Object o){if(this==o)return true; if(!(o instanceof Finding))return false; Finding f=(Finding)o; return Objects.equals(ruleId,f.ruleId)&&Objects.equals(category,f.category)&&severity==f.severity&&Objects.equals(targetStageId,f.targetStageId)&&Objects.equals(explanation,f.explanation)&&Objects.equals(evidence,f.evidence)&&Objects.equals(recommendations,f.recommendations);} @Override public int hashCode(){return Objects.hash(ruleId,category,severity,targetStageId,explanation,evidence,recommendations);} }
