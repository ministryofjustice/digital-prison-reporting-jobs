package uk.gov.justice.digital.service.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LatencyStatisticsTest {
    @Test
    void isEmptyShouldReportNonEmptyForValidStatistics() {
        assertFalse(LatencyStatistics.isEmpty(new LatencyStatistics(1L, 1L, 1L, 1L)));
    }

    @Test
    void isEmptyShouldReportEmptyForEmptyStatistics() {
        assertTrue(LatencyStatistics.isEmpty(LatencyStatistics.EMPTY));
    }

    @Test
    void isEmptyShouldReportEmptyForAllZeroStatistics() {
        assertTrue(LatencyStatistics.isEmpty(new LatencyStatistics(0L, 0L, 0L, 0L)));
    }
}
