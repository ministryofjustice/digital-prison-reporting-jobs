package uk.gov.justice.digital.service.datareconciliation;

import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

public interface MetricReportingService {
    void reportMetrics(DataReconciliationResults dataReconciliationResults);
}
