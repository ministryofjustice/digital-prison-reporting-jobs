package uk.gov.justice.digital.service.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

public class DisabledBatchMetrics implements BatchMetrics {
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
    public void close() {
        // No op
    }
}
