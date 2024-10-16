package uk.gov.justice.digital.service.datareconciliation;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.cloudwatch.CloudwatchClient;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

@Singleton
@Requires(property = "dpr.reconciliation.report.results.to.cloudwatch")
public class ReconciliationMetricReportingServiceImpl implements ReconciliationMetricReportingService {

    // todo: replace with configured value
    private static final String NAMESPACE = "";
    private final CloudwatchClient cloudwatchClient;

    @Inject
    public ReconciliationMetricReportingServiceImpl(
            CloudwatchClient cloudwatchClient
    ) {
        this.cloudwatchClient = cloudwatchClient;
    }

    @Override
    public void reportMetrics(DataReconciliationResults dataReconciliationResults) {
        cloudwatchClient.putMetrics(NAMESPACE, dataReconciliationResults.toCloudwatchMetricData());
    }
}
