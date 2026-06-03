package io.sparkadvisor.ui.render;

import io.sparkadvisor.core.util.Strings;

import org.apache.spark.SparkConf;

/**
 * Resolves the HDFS event-log path for an application served by the History Server.
 *
 * <p>The SHS reads logs from {@code spark.history.fs.logDirectory}. The UI only has the app id
 * from {@code SparkUI}, so this resolver returns the app-id-shaped base candidate
 * {@code <logDir>/<appId>}. Downstream readers resolve that candidate to the concrete log object,
 * including Spark's rolling directory form {@code eventlog_v2_<appId>} and in-progress/suffixed
 * single files.
 *
 * <p>VERIFY@3.5.1: config key {@code spark.history.fs.logDirectory} and appId/attempt shape.
 */
public final class EventLogPathResolver {

    private static final String LOG_DIR_KEY = "spark.history.fs.logDirectory";

    private final String logDir;

    public EventLogPathResolver(SparkConf conf) {
        this.logDir = conf.get(LOG_DIR_KEY, "");
    }

    public EventLogPathResolver(String logDir) {
        this.logDir = logDir == null ? "" : logDir;
    }

    /**
     * Candidate path for an application's event log. Returns {@code <logDir>/<appId>}; for
     * applications with attempts, callers may append the attempt id.
     */
    public String pathFor(String appId) {
        String base = stripTrailingSlash(logDir);
        return base.isEmpty() ? appId : base + "/" + appId;
    }

    /** Path including an attempt id, when present: {@code <logDir>/<appId>_<attemptId>}. */
    public String pathFor(String appId, String attemptId) {
        if (Strings.isBlank(attemptId)) {
            return pathFor(appId);
        }
        return pathFor(appId) + "_" + attemptId;
    }

    public boolean isConfigured() {
        return !Strings.isBlank(logDir);
    }

    private static String stripTrailingSlash(String s) {
        if (s == null) return "";
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }
}
