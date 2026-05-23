package io.sparkadvisor.cli;

import io.sparkadvisor.core.EventLogAnalyzer;
import io.sparkadvisor.core.locate.SqlLocator;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.report.html.HtmlReportWriter;
import io.sparkadvisor.report.json.JsonReportWriter;
import io.sparkadvisor.report.model.AnalysisResult;
import io.sparkadvisor.report.model.AnalysisResultBuilder;

import org.apache.hadoop.conf.Configuration;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * {@code sparkadvisor analyze} — read an event log, locate the target SQL (by StatementID
 * or executionId, else the slowest SQLs), build the {@link AnalysisResult} contract, and
 * render it to HTML or JSON.
 */
@Command(
        name = "analyze",
        mixinStandardHelpOptions = true,
        description = "Parse an event log and produce an analysis report.")
public final class AnalyzeCommand implements Callable<Integer> {

    @Option(names = "--path", required = true,
            description = "HDFS path to the event log (single file or rolling directory).")
    String path;

    @Option(names = "--statement-id",
            description = "StatementID from the leading /* ... */ comment. "
                    + "A numeric value with no StatementID match falls back to executionId.")
    String statementId;

    @Option(names = "--format", defaultValue = "html",
            description = "Output format: html | json. Default: html.")
    String format;

    @Option(names = "--out", description = "Output file path. Defaults to ./report.<format>.")
    String out;

    @Option(names = "--top", defaultValue = "5",
            description = "When no --statement-id is given, analyze the N slowest SQLs.")
    int top;

    @Option(names = "--keep-raw", defaultValue = "false",
            description = "Debug: retain raw task records (high memory).")
    boolean keepRaw;

    @Option(names = "--hadoop-conf-dir",
            description = "Override HADOOP_CONF_DIR (otherwise inherited from the environment).")
    String hadoopConfDir;

    @Option(names = "--advise", defaultValue = "rule",
            description = "Tuning advisor: none | rule | llm. Default: rule (offline). "
                    + "'llm' calls an LLM (needs ANTHROPIC_API_KEY) and consumes the structured "
                    + "analysis, never the raw log.")
    String advise;

    @Override
    public Integer call() throws Exception {
        Configuration conf = new Configuration();
        if (hadoopConfDir != null && !hadoopConfDir.isBlank()) {
            conf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDir + "/core-site.xml"));
            conf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDir + "/hdfs-site.xml"));
        }

        EventLogAnalyzer analyzer = new EventLogAnalyzer(conf);
        ApplicationModel model = analyzer.analyze(path);

        if (model.incomplete()) {
            System.err.println("[warn] Event log appears incomplete/truncated; "
                    + "some metrics may be missing.");
        }

        SqlExecution target = selectTarget(model);
        if (target == null && (statementId != null && !statementId.isBlank())) {
            System.err.println("[error] No SQL execution matched StatementID/executionId: "
                    + statementId);
            return 2;
        }

        AnalysisResult result = new AnalysisResultBuilder(model, path).build(target);

        // Apply the selected advisor (consumes the structured result, never the raw log).
        io.sparkadvisor.advisor.api.TuningAdvisor advisor =
                io.sparkadvisor.advisor.AdvisorFactory.forMode(advise);
        if (advisor != null && result.targetSql() != null) {
            result = result.withAiAdvice(advisor.advise(result));
        }

        String fmt = format == null ? "html" : format.toLowerCase();
        Path outPath = Path.of(out != null ? out : "report." + fmt);
        switch (fmt) {
            case "json" -> new JsonReportWriter().write(result, outPath);
            case "html" -> new HtmlReportWriter().write(result, outPath);
            default -> {
                System.err.println("[error] Unknown --format: " + format + " (use html|json)");
                return 2;
            }
        }
        System.out.println("Report written: " + outPath.toAbsolutePath());
        return 0;
    }

    /** Resolve the single target SQL: by id if given, else the slowest one. */
    private SqlExecution selectTarget(ApplicationModel model) {
        if (statementId != null && !statementId.isBlank()) {
            List<SqlExecution> matches = new SqlLocator(model).locate(statementId);
            return matches.isEmpty() ? null : matches.get(0);
        }
        return model.sqlExecutions().stream()
                .sorted(java.util.Comparator.comparingLong(SqlExecution::wallClockMs).reversed())
                .findFirst()
                .orElse(null);
    }
}

