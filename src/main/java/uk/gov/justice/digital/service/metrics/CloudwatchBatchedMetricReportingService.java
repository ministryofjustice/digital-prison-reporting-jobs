package uk.gov.justice.digital.service.metrics;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.cloudwatch.CloudwatchClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;

import java.util.function.Consumer;
import java.util.function.Function;

import static uk.gov.justice.digital.config.JobArguments.REPORT_METRICS_TO_CLOUDWATCH;

@Singleton
@Requires(property = REPORT_METRICS_TO_CLOUDWATCH)
public class CloudwatchBatchedMetricReportingService implements BatchedMetricReportingService {

    private final JobArguments jobArguments;
    private final JobProperties jobProperties;
    private final CloudwatchClient cloudwatchClient;


    @Inject
    public CloudwatchBatchedMetricReportingService(
            JobArguments jobArguments,
            JobProperties jobProperties,
            CloudwatchClient cloudwatchClient
    ) {
        this.jobArguments = jobArguments;
        this.jobProperties = jobProperties;
        this.cloudwatchClient = cloudwatchClient;
    }

    @Override
    public void withBatchedMetrics(Consumer<BatchMetrics> func) {
        try(BatchMetrics context = createMetricReportingContext()) {
            func.accept(context);
        }
    }

    @Override
    public <T> T withBatchedMetrics(Function<BatchMetrics, T> func) {
        try(BatchMetrics context = createMetricReportingContext()) {
            return func.apply(context);
        }
    }

    private BatchMetrics createMetricReportingContext() {
        return new CloudwatchBatchMetrics(jobArguments, jobProperties, cloudwatchClient);
    }
}
