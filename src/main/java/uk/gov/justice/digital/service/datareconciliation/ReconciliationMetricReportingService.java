package uk.gov.justice.digital.service.datareconciliation;

import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

public interface ReconciliationMetricReportingService {
    void reportMetrics(DataReconciliationResults dataReconciliationResults);
}
