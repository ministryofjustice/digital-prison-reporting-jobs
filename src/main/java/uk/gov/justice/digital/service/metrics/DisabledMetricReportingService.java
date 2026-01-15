package uk.gov.justice.digital.service.metrics;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import static uk.gov.justice.digital.config.JobArguments.REPORT_RESULTS_TO_CLOUDWATCH;

@Singleton
@Requires(missingProperty = REPORT_RESULTS_TO_CLOUDWATCH)
public class DisabledMetricReportingService implements MetricReportingService {
    @Override
    public void reportDataReconciliationResults(DataReconciliationResults dataReconciliationResults) {
        // No op
    }
}
