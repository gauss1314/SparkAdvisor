package io.sparkadvisor.cli;

import io.sparkadvisor.monitor.QueueAnalyzer;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.monitor.advisor.QueueAdvisorFactory;
import io.sparkadvisor.monitor.advisor.QueueLlmAdvisor;
import io.sparkadvisor.monitor.render.QueueHtmlWriter;
import io.sparkadvisor.monitor.render.QueueJsonWriter;
import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.report.i18n.ReportLanguage;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

/**
 * {@code sparkadvisor queue-report} — analyze a full long-running query-queue application
 * and emit a queue-level HTML/JSON report.
 */
@Command(
        name = "queue-report",
        mixinStandardHelpOptions = true,
        description = "Analyze all SQL executions in one Spark application as a fixed-resource query queue.")
public final class QueueReportCommand implements Callable<Integer> {

    @Option(names = "--path", required = true,
            description = "HDFS path to the queue application's event log.")
    String path;

    @Option(names = "--format", defaultValue = "html",
            description = "Output format: html | json. Default: html.")
    String format;

    @Option(names = "--out",
            description = "Output file path. Defaults to ./queue-report.<format>.")
    String out;

    @Option(names = "--top", defaultValue = "50",
            description = "Number of slowest completed SQLs to deeply analyze. Default: 50.")
    int top;

    @Option(names = "--sample-per-stratum", defaultValue = "5",
            description = "Additional deep-analysis samples per stratum (spill/fetch/GC/skew/template). Default: 5.")
    int samplePerStratum;

    @Option(names = "--bucket", defaultValue = "1h",
            description = "Timeline bucket size, e.g. 15m, 1h, 3600s. Default: 1h.")
    String bucket;

    @Option(names = "--hadoop-conf-dir",
            description = "Override HADOOP_CONF_DIR (otherwise inherited from the environment).")
    String hadoopConfDir;

    @Option(names = "--auth-to-local",
            description = "Override Hadoop hadoop.security.auth_to_local rules, e.g. "
                    + "'RULE:[1:$1@$0](.*@HADOOP.COM)s/@.*// DEFAULT'.")
    String authToLocal;

    @Option(names = "--advise", defaultValue = "none",
            description = "Queue AI advisor mode: none | llm. Default: none. llm uses MiniMax-M2.5 unless overridden.")
    String advise;

    @Option(names = "--lang", defaultValue = "auto",
            description = "Report language: auto | zh | en. Default: auto. "
                    + "In auto mode, an output filename containing '_zh' renders Chinese.")
    String lang;

    @Override
    public Integer call() throws Exception {
        org.apache.hadoop.conf.Configuration conf =
                HadoopCliConfiguration.load(hadoopConfDir, authToLocal);

        QueueAnalysisResult result = new QueueAnalyzer(conf)
                .analyze(path, top, samplePerStratum, parseDurationMs(bucket));
        String fmt = format == null ? "html" : format.toLowerCase();
        Path outPath = Paths.get(out != null ? out : "queue-report." + fmt);
        ReportLanguage reportLanguage = ReportLanguage.resolve(lang, outPath);
        QueueLlmAdvisor advisor = QueueAdvisorFactory.forMode(advise, reportLanguage);
        if (advisor != null) {
            result = result.withAiAdvice(advisor.advise(result));
        }
        if ("json".equals(fmt)) {
            new QueueJsonWriter().write(result, outPath);
        } else if ("html".equals(fmt)) {
            new QueueHtmlWriter().write(result, outPath, reportLanguage);
        } else {
            System.err.println("[error] Unknown --format: " + format + " (use html|json)");
            return 2;
        }
        System.out.println("Queue report written: " + outPath.toAbsolutePath());
        return 0;
    }

    static long parseDurationMs(String value) {
        if (Strings.isBlank(value)) {
            return QueueAnalyzer.DEFAULT_BUCKET_MS;
        }
        String v = value.trim().toLowerCase();
        long multiplier = 1L;
        if (v.endsWith("ms")) {
            v = v.substring(0, v.length() - 2);
        } else if (v.endsWith("s")) {
            multiplier = 1000L;
            v = v.substring(0, v.length() - 1);
        } else if (v.endsWith("m")) {
            multiplier = 60_000L;
            v = v.substring(0, v.length() - 1);
        } else if (v.endsWith("h")) {
            multiplier = 60L * 60L * 1000L;
            v = v.substring(0, v.length() - 1);
        }
        try {
            long parsed = Long.parseLong(v.trim()) * multiplier;
            return Math.max(60_000L, parsed);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid --bucket value: " + value);
        }
    }
}
