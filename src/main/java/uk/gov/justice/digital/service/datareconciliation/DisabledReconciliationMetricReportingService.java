package uk.gov.justice.digital.service.datareconciliation;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

@Singleton
@Requires(missingProperty = "dpr.reconciliation.report.results.to.cloudwatch")
public class DisabledReconciliationMetricReportingService implements ReconciliationMetricReportingService {
    @Override
    public void reportMetrics(DataReconciliationResults dataReconciliationResults) {
        // No op
    }

    @Override
    public void reportMetrics(ChangeDataTotalCounts changeDataTotalCounts) {
        // No op
    }

    @Override
    public void reportMetrics(CurrentStateTotalCounts currentStateTotalCounts) {
        // No op
    }
}