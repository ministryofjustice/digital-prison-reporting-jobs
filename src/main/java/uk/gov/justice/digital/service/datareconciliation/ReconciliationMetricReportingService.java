package uk.gov.justice.digital.service.datareconciliation;

import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

public interface ReconciliationMetricReportingService {
    void reportMetrics(DataReconciliationResults dataReconciliationResults);
    void reportMetrics(ChangeDataTotalCounts changeDataTotalCounts);
    void reportMetrics(CurrentStateTotalCounts currentStateTotalCounts);
}