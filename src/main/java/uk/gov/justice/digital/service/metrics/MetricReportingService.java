package uk.gov.justice.digital.service.metrics;

import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

public interface MetricReportingService {
    void reportViolationCount(long count);
    void reportDataReconciliationResults(DataReconciliationResults dataReconciliationResults);
}
