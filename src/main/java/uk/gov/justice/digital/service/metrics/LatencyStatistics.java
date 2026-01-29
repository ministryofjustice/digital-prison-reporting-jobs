package uk.gov.justice.digital.service.metrics;

import lombok.Data;

@Data
public class LatencyStatistics {

    // Represents an empty set of statistics, e.g. for when there is no valid latency data available
    public static final LatencyStatistics EMPTY = new LatencyStatistics(null, null, null, null);

    public static boolean isEmpty(LatencyStatistics latencyStatistics) {
        return latencyStatistics == null || latencyStatistics == EMPTY;
    }

    private final Long minimum;
    private final Long maximum;
    private final Long sum;
    private final Long totalCount;
}
