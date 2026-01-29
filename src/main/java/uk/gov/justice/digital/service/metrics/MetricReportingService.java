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
    void reportStreamingThroughputInput(long count);
    void reportStreamingThroughputWrittenToStructured(long count);
    void reportStreamingThroughputWrittenToCurated(long count);
    void reportStreamingMicroBatchTimeTaken(long timeTakenMs);
    void reportStreamingLatencyDmsToCurated(LatencyStatistics latencyStatistics);
}
