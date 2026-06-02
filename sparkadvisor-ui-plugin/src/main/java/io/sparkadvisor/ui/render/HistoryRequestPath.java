package io.sparkadvisor.ui.render;

import io.sparkadvisor.core.util.Strings;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Extracts the History Server application identity from request URIs.
 *
 * <p>History Server application pages are served under {@code .../history/<appId>/...} or,
 * for attempt-specific URLs, {@code .../history/<appId>/<attemptId>/...}. Some SHS-created
 * {@code SparkUI} instances can expose a blank/null {@code appId()}, so the UI plugin keeps
 * this request-path fallback to avoid resolving event logs as {@code <logDir>/null}.
 *
 * <p>VERIFY@3.5.1: History Server application URL shape.
 */
public final class HistoryRequestPath {

    private static final String HISTORY_SEGMENT = "history";
    private static final String SPARKADVISOR_SEGMENT = "sparkadvisor";

    private final String appId;
    private final String attemptId;

    private HistoryRequestPath(String appId, String attemptId) {
        this.appId = appId;
        this.attemptId = attemptId;
    }

    public static HistoryRequestPath empty() {
        return new HistoryRequestPath("", "");
    }

    public static HistoryRequestPath fromRequestUri(String requestUri) {
        if (Strings.isBlank(requestUri)) {
            return empty();
        }
        String uri = stripQuery(requestUri);
        String[] segments = uri.split("/");
        for (int i = 0; i < segments.length; i++) {
            String segment = decode(segments[i]);
            if (!HISTORY_SEGMENT.equals(segment)) {
                continue;
            }
            String appId = nextDecodedSegment(segments, i + 1);
            if (Strings.isBlank(appId)) {
                return empty();
            }
            String next = nextDecodedSegment(segments, i + 2);
            String attemptId = SPARKADVISOR_SEGMENT.equals(next) ? "" : next;
            return new HistoryRequestPath(appId, attemptId);
        }
        return empty();
    }

    public String appId() {
        return appId;
    }

    public String attemptId() {
        return attemptId;
    }

    public boolean hasAppId() {
        return !Strings.isBlank(appId);
    }

    private static String stripQuery(String uri) {
        int query = uri.indexOf('?');
        return query >= 0 ? uri.substring(0, query) : uri;
    }

    private static String nextDecodedSegment(String[] segments, int start) {
        for (int i = start; i < segments.length; i++) {
            String decoded = decode(segments[i]);
            if (!Strings.isBlank(decoded)) {
                return decoded;
            }
        }
        return "";
    }

    private static String decode(String segment) {
        if (segment == null) {
            return "";
        }
        try {
            return URLDecoder.decode(segment, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return segment;
        }
    }
}
