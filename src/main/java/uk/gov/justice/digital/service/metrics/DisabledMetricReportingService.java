package uk.gov.justice.digital.service.metrics;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import static uk.gov.justice.digital.config.JobArguments.REPORT_METRICS_TO_CLOUDWATCH;

@Singleton
@Requires(missingProperty = REPORT_METRICS_TO_CLOUDWATCH)
public class DisabledMetricReportingService implements MetricReportingService {
    @Override
    public void reportViolationCount(long count) {
        // No op
    }

    @Override
    public void reportDataReconciliationResults(DataReconciliationResults dataReconciliationResults) {
        // No op
    }

    @Override
    public void reportStreamingThroughputInput(Dataset<Row> inputDf) {
        // No op
    }

    @Override
    public void reportStreamingThroughputWrittenToStructured(Dataset<Row> structuredDf) {
        // No op
    }

    @Override
    public void reportStreamingThroughputWrittenToCurated(Dataset<Row> curatedDf) {
        // No op
    }
}
