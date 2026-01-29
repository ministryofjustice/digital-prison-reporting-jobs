package uk.gov.justice.digital.service.metrics;

import lombok.Data;

@Data
public class LatencyStatistics {
    private final Long minimum;
    private final Long maximum;
    private final Long sum;
    private final Long totalCount;
}
