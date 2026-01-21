package uk.gov.justice.digital.service.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

/**
 * Responsible for publishing metrics related to Glue jobs.
 */
public interface MetricReportingService {
    void reportViolationCount(long count);
    void reportDataReconciliationResults(DataReconciliationResults dataReconciliationResults);
    void reportStreamingThroughputInput(Dataset<Row> inputDf);
    void reportStreamingThroughputWrittenToStructured(Dataset<Row> structuredDf);
    void reportStreamingThroughputWrittenToCurated(Dataset<Row> curatedDf);
    void reportStreamingMicroBatchTimeTaken(long timeTakenMs);
}
