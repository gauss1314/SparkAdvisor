package io.sparkadvisor.ui.render;

import io.sparkadvisor.monitor.QueueAnalysisContext;
import io.sparkadvisor.monitor.QueueAnalyzer;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.monitor.checkpoint.EventLogSnapshot;
import io.sparkadvisor.monitor.checkpoint.ReplayCheckpoint;
import io.sparkadvisor.monitor.render.QueueHtmlWriter;
import io.sparkadvisor.report.i18n.ReportLanguage;

import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Async single-flight coordinator for queue reports in the History Server tab.
 *
 * <p>Queue logs can be many GB, so the UI request thread only checks the snapshot key and
 * either renders a cached result or schedules background parsing.
 */
public final class QueueAnalysisCoordinator {

    private static final int MAX_CACHED_RESULTS = 8;
    private static final long ANALYSIS_TIMEOUT_MINUTES = 30L;

    private final Configuration hadoopConf;
    private final QueueAnalyzer analyzer;
    private final QueueHtmlWriter htmlWriter = new QueueHtmlWriter();
    private final ReplayCheckpoint checkpoint = new ReplayCheckpoint();
    private final ConcurrentHashMap<String, QueueAnalysisResult> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<QueueAnalysisResult>> inFlight =
            new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(2, daemonFactory());
    private final ScheduledExecutorService timeoutExecutor =
            Executors.newSingleThreadScheduledExecutor(daemonFactory());

    public QueueAnalysisCoordinator(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
        this.analyzer = new QueueAnalyzer(hadoopConf);
    }

    public String stylesheet() {
        return htmlWriter.stylesheet();
    }

    public String renderBody(String path, int topN, long bucketMs) throws Exception {
        return renderBody(path, topN, bucketMs, ReportLanguage.EN);
    }

    public String renderBody(String path, int topN, long bucketMs, ReportLanguage language) throws Exception {
        return renderBody(path, topN, QueueAnalyzer.DEFAULT_SAMPLE_PER_STRATUM, bucketMs, language);
    }

    public String renderBody(String path, int topN, int samplePerStratum, long bucketMs,
                             ReportLanguage language) throws Exception {
        int normalizedTopN = Math.max(1, topN);
        int normalizedSamplePerStratum = Math.max(0, samplePerStratum);
        long normalizedBucketMs = Math.max(60_000L, bucketMs);
        EventLogSnapshot snapshot = EventLogSnapshot.fromPath(path, hadoopConf);
        String analysisKey = analysisKey(snapshot, normalizedTopN, normalizedSamplePerStratum, normalizedBucketMs);
        EventLogSnapshot checkpointSnapshot = snapshot.withKey(analysisKey);
        QueueAnalysisResult cached = cache.get(analysisKey);
        if (cached != null) {
            return htmlWriter.renderBody(cached, language);
        }
        String checkpointed = checkpoint.readHtml(checkpointSnapshot, language);
        if (checkpointed != null) {
            return checkpointed;
        }

        CompletableFuture<QueueAnalysisResult> future = inFlight.computeIfAbsent(analysisKey,
                key -> withTimeout(CompletableFuture.supplyAsync(() -> {
                    try {
                        QueueAnalysisResult result = analyzer.analyze(path, normalizedTopN,
                                normalizedSamplePerStratum, normalizedBucketMs,
                                QueueAnalysisContext.fullSnapshot(snapshot.key(),
                                        "Safe byte-offset replay is not enabled; this History Server "
                                                + "report was produced by a full event-log snapshot replay "
                                                + "and cached by snapshot plus analysis parameters."));
                        if (cache.size() >= MAX_CACHED_RESULTS) {
                            cache.clear();
                        }
                        cache.put(key, result);
                        checkpoint.writeHtml(checkpointSnapshot,
                                htmlWriter.renderBody(result, ReportLanguage.EN),
                                htmlWriter.renderBody(result, ReportLanguage.ZH));
                        return result;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        inFlight.remove(key);
                    }
                }, executor), ANALYSIS_TIMEOUT_MINUTES, TimeUnit.MINUTES));

        if (future.isDone() && !future.isCompletedExceptionally()) {
            QueueAnalysisResult result = future.get();
            cache.put(analysisKey, result);
            return htmlWriter.renderBody(result, language);
        }
        if (future.isCompletedExceptionally()) {
            inFlight.remove(analysisKey);
            return language != null && language.isChinese()
                    ? "<div class=\"banner warn\">队列分析失败或超过 "
                    + ANALYSIS_TIMEOUT_MINUTES + " 分钟超时。刷新页面可重试。</div>"
                    : "<div class=\"banner warn\">Queue analysis failed or timed out after "
                    + ANALYSIS_TIMEOUT_MINUTES + " minutes. Refresh to retry.</div>";
        }
        return inProgressHtml(snapshot, language);
    }

    private String analysisKey(EventLogSnapshot snapshot, int topN, int samplePerStratum, long bucketMs) {
        return snapshot.key() + "|top=" + topN + "|samplePerStratum=" + samplePerStratum
                + "|bucketMs=" + bucketMs;
    }

    private String inProgressHtml(EventLogSnapshot snapshot, ReportLanguage language) {
        if (language != null && language.isChinese()) {
            return "<div class=\"banner warn\">队列分析正在后台运行。快照大小："
                    + bytes(snapshot.totalBytes())
                    + "。稍后刷新该 tab 查看缓存后的队列报告。</div>";
        }
        return "<div class=\"banner warn\">Queue analysis is running in the background. "
                + "Snapshot size: " + bytes(snapshot.totalBytes())
                + ". Refresh this tab later to view the cached queue report.</div>";
    }

    private <T> CompletableFuture<T> withTimeout(CompletableFuture<T> future, long timeout, TimeUnit unit) {
        timeoutExecutor.schedule(() -> future.completeExceptionally(
                new RuntimeException("Timed out after " + timeout + " " + unit.toString().toLowerCase())),
                timeout, unit);
        return future;
    }

    private static ThreadFactory daemonFactory() {
        return r -> {
            Thread t = new Thread(r, "sparkadvisor-queue-analysis");
            t.setDaemon(true);
            return t;
        };
    }

    private static String bytes(long v) {
        if (v < 1024) return v + " B";
        double d = v;
        String[] units = {"KB", "MB", "GB", "TB"};
        int i = -1;
        do {
            d /= 1024.0;
            i++;
        } while (d >= 1024.0 && i < units.length - 1);
        return String.format("%.2f %s", d, units[i]);
    }

}
