package io.sparkadvisor.ui.render;

import io.sparkadvisor.advisor.AdvisorFactory;
import io.sparkadvisor.advisor.api.TuningAdvisor;
import io.sparkadvisor.core.EventLogAnalyzer;
import io.sparkadvisor.core.locate.SqlLocator;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.report.html.HtmlReportWriter;
import io.sparkadvisor.report.i18n.ReportLanguage;
import io.sparkadvisor.report.model.AnalysisResult;
import io.sparkadvisor.report.model.AnalysisResultBuilder;

import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinates the full SparkAdvisor pipeline for the History Server tab: parse the event log
 * (lazily, cached per path), locate the SQL by StatementID, build the {@link AnalysisResult},
 * and render the HTML body.
 *
 * <p>Parsing a GB-scale log is expensive, so the parsed {@link ApplicationModel} is cached per
 * path. The cache is bounded by simple size eviction (oldest cleared) to avoid unbounded
 * memory growth in a long-running History Server.
 */
public final class AnalysisCoordinator {

    private static final int MAX_CACHED_APPS = 16;

    private final Configuration hadoopConf;
    private final EventLogAnalyzer analyzer;
    private final HtmlReportWriter htmlWriter = new HtmlReportWriter();
    private final ConcurrentHashMap<String, ApplicationModel> cache = new ConcurrentHashMap<>();

    public AnalysisCoordinator(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
        this.analyzer = new EventLogAnalyzer(hadoopConf);
    }

    /** Parsed model for a path, parsing+caching on first access. */
    public ApplicationModel modelFor(String path) throws Exception {
        ApplicationModel cached = cache.get(path);
        if (cached != null) {
            return cached;
        }
        ApplicationModel model = analyzer.analyze(path);
        if (cache.size() >= MAX_CACHED_APPS) {
            cache.clear(); // coarse eviction; refine to LRU later if needed
        }
        cache.put(path, model);
        return model;
    }

    /** The CSS for the report body (inlined by the page). */
    public String stylesheet() {
        return htmlWriter.stylesheet();
    }

    /**
     * Render the report body for a given StatementID. Returns the inner HTML (no page chrome).
     *
     * @param path        event-log path
     * @param statementId StatementID or numeric executionId; if null, the slowest SQL is used
     */
    public String renderBody(String path, String statementId) throws Exception {
        return renderBody(path, statementId, ReportLanguage.EN);
    }

    public String renderBody(String path, String statementId, ReportLanguage language) throws Exception {
        ApplicationModel model = modelFor(path);
        SqlExecution target = selectTarget(model, statementId);
        AnalysisResult result = new AnalysisResultBuilder(model, path).build(target);
        if (result.targetSql() != null) {
            TuningAdvisor advisor = AdvisorFactory.forMode("rule", language);
            result = result.withAiAdvice(advisor.advise(result));
        }
        return htmlWriter.renderBody(result, language);
    }

    /** List the StatementIDs available in this app, for hints in the UI. */
    public List<String> availableStatementIds(String path) throws Exception {
        return modelFor(path).sqlExecutions().stream()
                .map(SqlExecution::statementId)
                .filter(s -> !Strings.isBlank(s))
                .distinct()
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
    }

    private SqlExecution selectTarget(ApplicationModel model, String statementId) {
        if (!Strings.isBlank(statementId)) {
            List<SqlExecution> matches = new SqlLocator(model).locate(statementId);
            return matches.isEmpty() ? null : matches.get(0);
        }
        Optional<SqlExecution> slowest = model.sqlExecutions().stream()
                .max(java.util.Comparator.comparingLong(SqlExecution::wallClockMs));
        return slowest.orElse(null);
    }
}
