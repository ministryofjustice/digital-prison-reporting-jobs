package uk.gov.justice.digital.service.metrics;

import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

public interface MetricReportingService {
    void reportMetrics(DataReconciliationResults dataReconciliationResults);
}
