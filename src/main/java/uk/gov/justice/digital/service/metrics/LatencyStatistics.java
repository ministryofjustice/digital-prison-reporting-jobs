package uk.gov.justice.digital.service.metrics;

import lombok.Data;

@Data
public class LatencyStatistics {

    // Represents an empty set of statistics, e.g. for when there is no valid latency data available
    public static final LatencyStatistics EMPTY = new LatencyStatistics(0L, 0L, 0L, 0L);

    public static boolean isEmpty(LatencyStatistics latencyStatistics) {
        return latencyStatistics == null || latencyStatistics.equals(EMPTY);
    }

    private final long minimum;
    private final long maximum;
    private final long sum;
    private final long totalCount;
}
