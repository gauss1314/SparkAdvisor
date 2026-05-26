package io.sparkadvisor.ui.render;

import io.sparkadvisor.monitor.QueueAnalyzer;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.monitor.render.QueueHtmlWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final ConcurrentHashMap<String, QueueAnalysisResult> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<QueueAnalysisResult>> inFlight =
            new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(2, daemonFactory());

    public QueueAnalysisCoordinator(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
        this.analyzer = new QueueAnalyzer(hadoopConf);
    }

    public String stylesheet() {
        return htmlWriter.stylesheet();
    }

    public String renderBody(String path, int topN, long bucketMs) throws Exception {
        Snapshot snapshot = snapshot(path);
        QueueAnalysisResult cached = cache.get(snapshot.key());
        if (cached != null) {
            return htmlWriter.renderBody(cached);
        }

        CompletableFuture<QueueAnalysisResult> future = inFlight.computeIfAbsent(snapshot.key(),
                key -> CompletableFuture.supplyAsync(() -> {
                    try {
                        QueueAnalysisResult result = analyzer.analyze(path, topN, bucketMs);
                        if (cache.size() >= MAX_CACHED_RESULTS) {
                            cache.clear();
                        }
                        cache.put(key, result);
                        return result;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        inFlight.remove(key);
                    }
                }, executor).orTimeout(ANALYSIS_TIMEOUT_MINUTES, TimeUnit.MINUTES));

        if (future.isDone() && !future.isCompletedExceptionally()) {
            QueueAnalysisResult result = future.get();
            cache.put(snapshot.key(), result);
            return htmlWriter.renderBody(result);
        }
        if (future.isCompletedExceptionally()) {
            inFlight.remove(snapshot.key());
            return "<div class=\"banner warn\">Queue analysis failed or timed out after "
                    + ANALYSIS_TIMEOUT_MINUTES + " minutes. Refresh to retry.</div>";
        }
        return inProgressHtml(snapshot);
    }

    private Snapshot snapshot(String pathStr) throws Exception {
        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(hadoopConf);
        long totalBytes = 0L;
        long maxModificationTime = 0L;
        if (fs.isDirectory(path)) {
            List<FileStatus> statuses = new ArrayList<>();
            collectFiles(fs, path, statuses);
            for (FileStatus status : statuses) {
                totalBytes += status.getLen();
                maxModificationTime = Math.max(maxModificationTime, status.getModificationTime());
            }
        } else {
            FileStatus status = fs.getFileStatus(path);
            totalBytes = status.getLen();
            maxModificationTime = status.getModificationTime();
        }
        return new Snapshot(pathStr + ":" + totalBytes + ":" + maxModificationTime,
                totalBytes, maxModificationTime);
    }

    private void collectFiles(FileSystem fs, Path root, List<FileStatus> out) throws Exception {
        FileStatus[] statuses = fs.listStatus(root);
        java.util.Arrays.sort(statuses, Comparator.comparing(s -> s.getPath().toString()));
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                collectFiles(fs, status.getPath(), out);
            } else {
                out.add(status);
            }
        }
    }

    private String inProgressHtml(Snapshot snapshot) {
        return "<div class=\"banner warn\">Queue analysis is running in the background. "
                + "Snapshot size: " + bytes(snapshot.totalBytes())
                + ". Refresh this tab later to view the cached queue report.</div>";
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

    private static final class Snapshot {
        private final String key;
        private final long totalBytes;
        private final long modifiedAt;
        private Snapshot(String key, long totalBytes, long modifiedAt){this.key=key;this.totalBytes=totalBytes;this.modifiedAt=modifiedAt;}
        private String key(){return key;}
        private long totalBytes(){return totalBytes;}
        private long modifiedAt(){return modifiedAt;}
    }
}
