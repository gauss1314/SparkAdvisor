package io.sparkadvisor.analyzer.v2;

import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Severity;
import io.sparkadvisor.core.util.Java8Collections;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.List;
import java.util.Set;

/** Capability-aware evaluator for the stable S/Q/DQ rule catalog. */
public final class RuleEngineV2 {
    private final List<MetricRule> rules;
    private final RuleThresholdsV2 thresholds;
    private final List<Suppression> suppressions;

    public RuleEngineV2(List<MetricRule> rules,RuleThresholdsV2 thresholds,List<Suppression> suppressions){this.rules=Java8Collections.listCopy(rules);this.thresholds=thresholds;this.suppressions=suppressions==null?Collections.<Suppression>emptyList():Java8Collections.listCopy(suppressions);validate();}
    public static RuleEngineV2 sqlDefaults(RuleThresholdsV2 thresholds){return new RuleEngineV2(RuleCatalogV2.sqlAndDataQuality(),thresholds,Collections.<Suppression>emptyList());}
    public static RuleEngineV2 queueDefaults(RuleThresholdsV2 thresholds){return new RuleEngineV2(RuleCatalogV2.queue(),thresholds,Collections.<Suppression>emptyList());}

    public List<Finding> evaluate(List<MetricsContext> contexts){
        return evaluateDetailed(contexts).findings();
    }

    public RuleRunResult evaluateDetailed(List<MetricsContext> contexts){
        List<Finding> out=new ArrayList<Finding>();
        Map<String,Set<Capability>> missingByRule=new LinkedHashMap<String,Set<Capability>>();
        Set<String> evaluatedRules=new LinkedHashSet<String>();
        for(MetricsContext context:contexts){
            for(MetricRule rule:rules){
                if(rule.scope()!=context.scope())continue;
                if(!context.capabilities().containsAll(rule.requires())){
                    EnumSet<Capability> missing=EnumSet.noneOf(Capability.class);missing.addAll(rule.requires());missing.removeAll(context.capabilities());
                    Set<Capability> known=missingByRule.get(rule.id());if(known==null){known=EnumSet.noneOf(Capability.class);missingByRule.put(rule.id(),known);}known.addAll(missing);
                    continue;
                }
                evaluatedRules.add(rule.id());
                for(Finding finding:rule.evaluate(context,thresholds))out.add(applySuppression(finding,context));
            }
        }
        for(String id:evaluatedRules)missingByRule.remove(id);
        out=applyInteractions(out);
        Collections.sort(out,new Comparator<Finding>(){public int compare(Finding a,Finding b){if(a.suppressed()!=b.suppressed())return a.suppressed()?1:-1;int score=Double.compare(b.score(),a.score());if(score!=0)return score;int sev=Integer.compare(b.severity().ordinal(),a.severity().ordinal());return sev!=0?sev:a.ruleId().compareTo(b.ruleId());}});
        Map<String,Set<Capability>> immutable=new LinkedHashMap<String,Set<Capability>>();for(Map.Entry<String,Set<Capability>> e:missingByRule.entrySet())immutable.put(e.getKey(),Collections.unmodifiableSet(EnumSet.copyOf(e.getValue())));
        return new RuleRunResult(Java8Collections.listCopy(out),immutable);
    }

    public Set<String> ids(){Set<String> ids=new LinkedHashSet<String>();for(MetricRule rule:rules)ids.add(rule.id());return Collections.unmodifiableSet(ids);}
    /** Threshold keys present in configuration but not declared by this engine's rule subset. */
    public Set<String> unusedThresholdKeys(){Set<String> declared=new LinkedHashSet<String>();for(MetricRule rule:rules)declared.addAll(rule.thresholdKeys());Set<String> unused=new LinkedHashSet<String>(thresholds.keys());unused.removeAll(declared);return Collections.unmodifiableSet(unused);}
    private Finding applySuppression(Finding finding,MetricsContext context){for(Suppression suppression:suppressions)if(suppression.matches(finding.ruleId(),context,LocalDate.now()))return finding.withSuppression(suppression.reason());return finding;}
    private List<Finding> applyInteractions(List<Finding> source){
        boolean queueLimited=source.stream().anyMatch(f->"S-14".equals(f.ruleId())&&evidence(f,"queue.busy_ratio")>=thresholds.get("queue.busy_ratio"));
        boolean dqPartial=has(source,"DQ-01");boolean clockSkew=has(source,"DQ-02");boolean storm=has(source,"Q-16");boolean badHost=has(source,"Q-05");boolean badIo=has(source,"Q-12");boolean speculation=has(source,"S-28");boolean memory=has(source,"Q-14");boolean rowAmp=has(source,"S-26");
        List<Finding> out=new ArrayList<Finding>();
        for(Finding f:source){
            Finding adjusted=f;int sNumber=seriesNumber(f.ruleId(),"S-");
            if((queueLimited||storm)&&sNumber>=1&&sNumber<=13&&sNumber!=14){adjusted=lower(adjusted,"MEDIUM",queueLimited?"Queue wait dominates this execution; fix contention before SQL micro-tuning.":"Failure storm window may distort performance metrics.");}
            if(dqPartial&&(sNumber==1||sNumber==2||sNumber==7||sNumber==8||sNumber==11)){adjusted=capWarn(adjusted,"MEDIUM","Event completeness is insufficient; affected values may be understated.");}
            if(clockSkew&&("Q-05".equals(f.ruleId())||"Q-12".equals(f.ruleId())||"Q-13".equals(f.ruleId()))){adjusted=lower(adjusted,"LOW","Host timestamps are inconsistent; host-level attribution is uncertain.");}
            if("S-17".equals(f.ruleId())&&evidence(f,"join.critical_path_ratio")<=0.0){adjusted=adjusted.withQuality(Severity.INFO,adjusted.confidence(),append(adjusted.caveat(),"Join is not proven to be on the critical path."));}
            if(badHost&&badIo&&"Q-05".equals(f.ruleId()))adjusted=adjusted.withQuality(adjusted.severity(),adjusted.confidence(),append(adjusted.caveat(),"Q-12 corroborates a disk-related host cause."));
            if(speculation&&badHost&&"S-28".equals(f.ruleId()))adjusted=adjusted.withQuality(adjusted.severity(),adjusted.confidence(),append(adjusted.caveat(),"Q-05 indicates speculation may be masking a bad host."));
            if(memory&&rowAmp&&"Q-14".equals(f.ruleId()))adjusted=adjusted.withQuality(adjusted.severity(),adjusted.confidence(),append(adjusted.caveat(),"S-26 row amplification is the preferred root cause before global memory changes."));
            out.add(adjusted);
        }
        return out;
    }
    private static boolean has(List<Finding> fs,String id){for(Finding f:fs)if(id.equals(f.ruleId()))return true;return false;}
    private static int seriesNumber(String id,String prefix){if(id==null||!id.startsWith(prefix))return -1;try{return Integer.parseInt(id.substring(prefix.length()));}catch(NumberFormatException ex){return -1;}}
    private static Finding lower(Finding f,String confidence,String caveat){Severity severity=f.severity()==Severity.CRITICAL?Severity.WARN:f.severity()==Severity.WARN?Severity.INFO:Severity.INFO;return f.withQuality(severity,confidence,append(f.caveat(),caveat));}
    private static Finding capWarn(Finding f,String confidence,String caveat){return f.withQuality(f.severity()==Severity.CRITICAL?Severity.WARN:f.severity(),confidence,append(f.caveat(),caveat));}
    private static double evidence(Finding f,String key){try{return Double.parseDouble(f.evidence().get(key));}catch(Exception ex){return 0.0;}}
    private static String append(String base,String extra){return base==null||base.isEmpty()?extra:base+" "+extra;}
    private void validate(){Set<String> ids=new LinkedHashSet<String>();for(MetricRule rule:rules){if(!ids.add(rule.id()))throw new IllegalArgumentException("Duplicate rule id: "+rule.id());for(String key:rule.thresholdKeys())if(!thresholds.contains(key))throw new IllegalArgumentException("Rule "+rule.id()+" references missing threshold "+key);}}
}
