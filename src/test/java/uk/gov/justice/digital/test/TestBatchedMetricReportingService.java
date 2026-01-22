package uk.gov.justice.digital.test;

import uk.gov.justice.digital.service.metrics.BatchMetrics;
import uk.gov.justice.digital.service.metrics.BatchedMetricReportingService;

import java.util.function.Consumer;
import java.util.function.Function;

public class TestBatchedMetricReportingService implements BatchedMetricReportingService {

    private final BatchMetrics batchMetrics;

    public TestBatchedMetricReportingService(BatchMetrics batchMetrics) {
        this.batchMetrics = batchMetrics;
    }

    @Override
    public void withBatchedMetrics(Consumer<BatchMetrics> func) {
        func.accept(batchMetrics);
    }

    @Override
    public <T> T withBatchedMetrics(Function<BatchMetrics, T> func) {
        return func.apply(batchMetrics);
    }
}
