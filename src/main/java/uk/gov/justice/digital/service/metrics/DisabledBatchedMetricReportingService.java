package uk.gov.justice.digital.service.metrics;

import io.micronaut.context.annotation.Requires;

import java.util.function.Consumer;
import java.util.function.Function;

import static uk.gov.justice.digital.config.JobArguments.REPORT_METRICS_TO_CLOUDWATCH;

@Requires(missingProperty = REPORT_METRICS_TO_CLOUDWATCH)
public class DisabledBatchedMetricReportingService implements BatchedMetricReportingService {
    @Override
    public void withBatchedMetrics(Consumer<BatchMetrics> func) {
        func.accept(new DisabledBatchMetrics());
    }

    @Override
    public <T> T withBatchedMetrics(Function<BatchMetrics, T> func) {
        return func.apply(new DisabledBatchMetrics());
    }
}
