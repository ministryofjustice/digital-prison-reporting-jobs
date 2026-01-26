package uk.gov.justice.digital.service.metrics;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import static uk.gov.justice.digital.config.JobArguments.REPORT_METRICS_TO_CLOUDWATCH;

@Singleton
@Requires(missingProperty = REPORT_METRICS_TO_CLOUDWATCH)
public class DisabledBufferedMetricReportingService implements BufferedMetricReportingService {
    @Override
    public void bufferViolationCount(long count) {
        // No op
    }

    @Override
    public void bufferDataReconciliationResults(DataReconciliationResults dataReconciliationResults) {
        // No op
    }

    @Override
    public void bufferStreamingThroughputInput(Dataset<Row> inputDf) {
        // No op
    }

    @Override
    public void bufferStreamingThroughputWrittenToStructured(Dataset<Row> structuredDf) {
        // No op
    }

    @Override
    public void bufferStreamingThroughputWrittenToCurated(Dataset<Row> curatedDf) {
        // No op
    }

    @Override
    public void bufferStreamingMicroBatchTimeTaken(long timeTakenMs) {
        // No op
    }

    @Override
    public void flushAllBufferedMetrics() {
        // No op
    }
}
