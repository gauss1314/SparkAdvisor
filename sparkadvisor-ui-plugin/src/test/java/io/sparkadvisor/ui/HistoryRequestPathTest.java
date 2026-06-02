package io.sparkadvisor.ui;

import io.sparkadvisor.ui.render.HistoryRequestPath;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HistoryRequestPathTest {

    @Test
    void extractsKubernetesStyleAppIdBehindProxyPrefix() {
        var parsed = HistoryRequestPath.fromRequestUri(
                "/spark/ui/history/spark-412e7687815849da9cf38719e24197f1/sparkadvisor/");

        assertTrue(parsed.hasAppId());
        assertEquals("spark-412e7687815849da9cf38719e24197f1", parsed.appId());
        assertEquals("", parsed.attemptId());
    }

    @Test
    void extractsAttemptIdWhenHistoryUrlIncludesAttemptSegment() {
        var parsed = HistoryRequestPath.fromRequestUri(
                "/history/application_1700000000000_0001/2/sparkadvisor/?statementId=s1");

        assertEquals("application_1700000000000_0001", parsed.appId());
        assertEquals("2", parsed.attemptId());
    }

    @Test
    void returnsEmptyWhenNoHistorySegmentExists() {
        var parsed = HistoryRequestPath.fromRequestUri("/sparkadvisor/");

        assertFalse(parsed.hasAppId());
        assertEquals("", parsed.appId());
        assertEquals("", parsed.attemptId());
    }
}
