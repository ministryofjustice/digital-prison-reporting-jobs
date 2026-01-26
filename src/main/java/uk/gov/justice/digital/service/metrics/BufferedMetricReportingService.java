package uk.gov.justice.digital.service.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

/**
 * Responsible for publishing metrics related to Glue jobs.
 */
public interface BufferedMetricReportingService {
    void bufferViolationCount(long count);
    void bufferDataReconciliationResults(DataReconciliationResults dataReconciliationResults);
    void bufferStreamingThroughputInput(Dataset<Row> inputDf);
    void bufferStreamingThroughputWrittenToStructured(Dataset<Row> structuredDf);
    void bufferStreamingThroughputWrittenToCurated(Dataset<Row> curatedDf);
    void bufferStreamingMicroBatchTimeTaken(long timeTakenMs);

    void flushAllBufferedMetrics();
}
