package io.sparkadvisor.cli;

import io.sparkadvisor.monitor.QueueAnalyzer;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.monitor.render.QueueHtmlWriter;
import io.sparkadvisor.monitor.render.QueueJsonWriter;

import org.apache.hadoop.conf.Configuration;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
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

    @Option(names = "--bucket", defaultValue = "1h",
            description = "Timeline bucket size, e.g. 15m, 1h, 3600s. Default: 1h.")
    String bucket;

    @Option(names = "--hadoop-conf-dir",
            description = "Override HADOOP_CONF_DIR (otherwise inherited from the environment).")
    String hadoopConfDir;

    @Override
    public Integer call() throws Exception {
        Configuration conf = new Configuration();
        if (hadoopConfDir != null && !hadoopConfDir.isBlank()) {
            conf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDir + "/core-site.xml"));
            conf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDir + "/hdfs-site.xml"));
        }

        QueueAnalysisResult result = new QueueAnalyzer(conf)
                .analyze(path, top, parseDurationMs(bucket));
        String fmt = format == null ? "html" : format.toLowerCase();
        Path outPath = Path.of(out != null ? out : "queue-report." + fmt);
        switch (fmt) {
            case "json" -> new QueueJsonWriter().write(result, outPath);
            case "html" -> new QueueHtmlWriter().write(result, outPath);
            default -> {
                System.err.println("[error] Unknown --format: " + format + " (use html|json)");
                return 2;
            }
        }
        System.out.println("Queue report written: " + outPath.toAbsolutePath());
        return 0;
    }

    static long parseDurationMs(String value) {
        if (value == null || value.isBlank()) {
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
