package uk.gov.justice.digital.service.metrics;

import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

/**
 * Responsible for publishing metrics related to Glue jobs.
 */
public interface MetricReportingService {
    void reportViolationCount(long count);
    void reportDataReconciliationResults(DataReconciliationResults dataReconciliationResults);
    void reportStreamingThroughputInput(long count);
    void reportStreamingThroughputWrittenToCurated(long count);
    void reportStreamingMicroBatchTimeTaken(long timeTakenMs);
    void reportStreamingLatencyDmsToCurated(LatencyStatistics latencyStatistics);
    void reportBatchJobTimeTaken(long timeTakenMs);
}
