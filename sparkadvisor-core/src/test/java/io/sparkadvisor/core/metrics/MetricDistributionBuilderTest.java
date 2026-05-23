package io.sparkadvisor.core.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricDistributionBuilderTest {

    @Test
    void emptyBuilderProducesEmptyDistribution() {
        assertEquals(Distribution.EMPTY, new MetricDistributionBuilder().build());
    }

    @Test
    void tracksCountSumMinMaxExactly() {
        var b = new MetricDistributionBuilder();
        for (long v : new long[]{10, 20, 30, 40, 50}) {
            b.add(v);
        }
        Distribution d = b.build();
        assertEquals(5, d.count());
        assertEquals(150, d.sum());
        assertEquals(10, d.min());
        assertEquals(50, d.max());
        assertEquals(30.0, d.mean(), 0.0001);
    }

    @Test
    void medianAndQuantilesAreReasonable() {
        var b = new MetricDistributionBuilder();
        for (long v = 1; v <= 100; v++) {
            b.add(v);
        }
        Distribution d = b.build();
        // t-digest is approximate; assert within a tolerant band around the true values.
        assertTrue(Math.abs(d.median() - 50) <= 3, "median ~50 but was " + d.median());
        assertTrue(Math.abs(d.p90() - 90) <= 3, "p90 ~90 but was " + d.p90());
        assertEquals(1, d.min());
        assertEquals(100, d.max());
    }

    @Test
    void skewRatioDetectsHeavyTail() {
        var b = new MetricDistributionBuilder();
        for (int i = 0; i < 99; i++) {
            b.add(100); // 99 normal tasks
        }
        b.add(10_000);  // one massive straggler
        Distribution d = b.build();
        // median stays ~100, max is 10000 -> skewRatio ~100
        assertTrue(d.skewRatio() > 50, "expected high skew but was " + d.skewRatio());
    }

    @Test
    void negativeValuesClampedToZero() {
        var b = new MetricDistributionBuilder();
        b.add(-5);
        b.add(5);
        Distribution d = b.build();
        assertEquals(0, d.min());
        assertEquals(5, d.max());
        assertEquals(5, d.sum());
    }
}
