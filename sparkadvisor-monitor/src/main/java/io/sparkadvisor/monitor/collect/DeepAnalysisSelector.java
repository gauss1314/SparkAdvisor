package io.sparkadvisor.monitor.collect;

import io.sparkadvisor.core.util.Java8Collections;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Selects which queue SQL samples receive full findings and predictions.
 *
 * <p>The selector keeps the slowest top-N, then adds representative samples from spill, fetch,
 * GC, skew, and repeated template strata so queue conclusions are not purely top-N biased.
 */
public final class DeepAnalysisSelector {

    private static final double FETCH_WAIT_STRATUM = 0.20;
    private static final double GC_STRATUM = 0.10;
    private static final double SKEW_STRATUM = 5.0;

    private final int topN;
    private final int samplePerStratum;

    public DeepAnalysisSelector(int topN, int samplePerStratum) {
        this.topN = Math.max(1, topN);
        this.samplePerStratum = Math.max(0, samplePerStratum);
    }

    public Set<Long> select(List<QuerySample> samples) {
        List<QuerySample> safe = samples == null ? Java8Collections.<QuerySample>listOf() : samples;
        Set<Long> selected = new LinkedHashSet<Long>();
        addTop(selected, safe, topN, byDurationDesc(), null);
        addTop(selected, safe, samplePerStratum, byDurationDesc(), new Filter() {
            public boolean include(QuerySample s) { return s.spillBytes() > 0L; }
        });
        addTop(selected, safe, samplePerStratum, byDurationDesc(), new Filter() {
            public boolean include(QuerySample s) { return s.fetchWaitRatio() >= FETCH_WAIT_STRATUM; }
        });
        addTop(selected, safe, samplePerStratum, byDurationDesc(), new Filter() {
            public boolean include(QuerySample s) { return s.maxGcRatio() >= GC_STRATUM; }
        });
        addTop(selected, safe, samplePerStratum, byDurationDesc(), new Filter() {
            public boolean include(QuerySample s) { return s.maxSkewRatio() >= SKEW_STRATUM; }
        });
        addTop(selected, onePerTemplate(safe), samplePerStratum, byDurationDesc(), null);
        return selected;
    }

    private static List<QuerySample> onePerTemplate(List<QuerySample> samples) {
        Map<String, QuerySample> byTemplate = new LinkedHashMap<String, QuerySample>();
        for (QuerySample sample : samples) {
            if (!eligible(sample)) {
                continue;
            }
            String key = sample.templateHash() == null ? "" : sample.templateHash();
            QuerySample existing = byTemplate.get(key);
            if (existing == null || sample.durationMs() > existing.durationMs()) {
                byTemplate.put(key, sample);
            }
        }
        return new ArrayList<QuerySample>(byTemplate.values());
    }

    private static Comparator<QuerySample> byDurationDesc() {
        return new Comparator<QuerySample>() {
            public int compare(QuerySample a, QuerySample b) {
                return Long.compare(b.durationMs(), a.durationMs());
            }
        };
    }

    private static void addTop(Set<Long> selected, List<QuerySample> samples, int limit,
                               Comparator<QuerySample> comparator, Filter filter) {
        samples.stream()
                .filter(DeepAnalysisSelector::eligible)
                .filter(s -> filter == null || filter.include(s))
                .sorted(comparator)
                .limit(Math.max(0, limit))
                .forEach(s -> selected.add(s.executionId()));
    }

    private static boolean eligible(QuerySample sample) {
        return sample != null && !sample.running() && sample.durationMs() > 0L;
    }

    private interface Filter {
        boolean include(QuerySample sample);
    }
}
