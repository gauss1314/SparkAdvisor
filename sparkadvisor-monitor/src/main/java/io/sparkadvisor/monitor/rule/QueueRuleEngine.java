package io.sparkadvisor.monitor.rule;

import io.sparkadvisor.analyzer.v2.MetricsContext;
import io.sparkadvisor.analyzer.v2.RuleCatalogV2;
import io.sparkadvisor.analyzer.v2.RuleEngineV2;
import io.sparkadvisor.analyzer.v2.RuleThresholdsV2;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.predict.Confidence;
import io.sparkadvisor.core.util.Java8Collections;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.monitor.collect.QuerySample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Queue implementation of the stable Q-01..Q-18 catalog in docs/rules.md. */
public final class QueueRuleEngine {
    private final RuleThresholdsV2 thresholds;
    public QueueRuleEngine(){this(RuleThresholdsV2.defaults());}
    public QueueRuleEngine(RuleThresholdsV2 thresholds){this.thresholds=thresholds;}

    /** Compatibility overload for tests/embedders without the source app. */
    public List<QueueAnalysisResult.QueueRecommendation> recommend(QueueAnalysisResult result){
        return recommend(result,QueueMetricsContextAdapter.from(result,null,Collections.<QuerySample>emptyList(),thresholds));
    }

    public List<QueueAnalysisResult.QueueRecommendation> recommend(QueueAnalysisResult result,ApplicationModel app,List<QuerySample> samples){
        MetricsContext context=QueueMetricsContextAdapter.from(result,app,samples,thresholds);
        return recommend(result,context);
    }

    private List<QueueAnalysisResult.QueueRecommendation> recommend(QueueAnalysisResult result,MetricsContext context){
        RuleEngineV2 engine=new RuleEngineV2(RuleCatalogV2.queue(),thresholds,Collections.emptyList());
        List<QueueAnalysisResult.QueueRecommendation> out=new ArrayList<QueueAnalysisResult.QueueRecommendation>();
        for(Finding finding:engine.evaluate(Collections.singletonList(context))){
            Recommendation recommendation=finding.recommendations().isEmpty()?Recommendation.governance(finding.explanation(),"Queue rule evidence","Review queue health"):finding.recommendations().get(0);
            out.add(new QueueAnalysisResult.QueueRecommendation(finding.ruleId(),recommendation,evidence(finding.evidence()),confidence(finding.confidence()),coverage(result),finding.caveat()));
        }
        return Java8Collections.listCopy(out);
    }

    private static String evidence(Map<String,String> evidence){StringBuilder b=new StringBuilder();for(Map.Entry<String,String> e:evidence.entrySet()){if(b.length()>0)b.append(", ");b.append(e.getKey()).append('=').append(e.getValue());}return b.toString();}
    private static Confidence confidence(String value){try{return Confidence.valueOf(value);}catch(Exception ex){return Confidence.LOW;}}
    private static String coverage(QueueAnalysisResult r){return r.meta().deepAnalyzedQueries()+" deep / "+r.meta().lightAnalyzedQueries()+" light queries";}
}
