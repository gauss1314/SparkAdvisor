package io.sparkadvisor.monitor.compat;

import io.sparkadvisor.core.model.TaskInterval;
import io.sparkadvisor.monitor.collect.QuerySample;
import io.sparkadvisor.monitor.contention.ContentionTimeline;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Java8BackportCompatibilityTest {

    @Test
    void queueValueObjectsKeepRecordLikeSemantics() {
        QuerySample one = query(1L);
        QuerySample two = query(1L);

        assertEquals(one, two);
        assertEquals(one.hashCode(), two.hashCode());
        assertTrue(one.toString().startsWith("QuerySample["));
    }

    @Test
    void contentionTimelineKeepsImmutableSnapshots() {
        QuerySample query = query(1L);
        TaskInterval task = new TaskInterval(1L, 1, 0, 1L, "1", 100L, 300L);

        ContentionTimeline timeline = ContentionTimeline.from(List.of(task), List.of(query),
                4, 100L, 300L, 60_000L);

        assertThrows(UnsupportedOperationException.class,
                () -> timeline.segments().add(new ContentionTimeline.Segment(1L, 2L, 1, 0.25)));
        assertThrows(UnsupportedOperationException.class,
                () -> timeline.bucketUtilization().add(
                        new ContentionTimeline.BucketUtilization(1L, 2L, 0.25)));
        assertThrows(UnsupportedOperationException.class,
                () -> timeline.queryContention().put(2L,
                        new ContentionTimeline.QueryContention(2L, 0.0, 0.0, 0L, false)));
        assertThrows(UnsupportedOperationException.class,
                () -> timeline.hotspots().add(new ContentionTimeline.Window(1L, 2L, 0.95)));
    }

    private static QuerySample query(long executionId) {
        return new QuerySample(executionId, "stmt", "select 1",
                100L, 300L, false, false, 200L, 1,
                0L, 0L, 0L, 0.5, 0.0, 0.0, null, List.of(), null);
    }
}
