package uk.gov.justice.digital.service.datareconciliation;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.cloudwatch.CloudwatchClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

@Singleton
@Requires(property = "dpr.reconciliation.report.results.to.cloudwatch")
public class ReconciliationMetricReportingServiceImpl implements ReconciliationMetricReportingService {

    private final JobArguments jobArguments;
    private final CloudwatchClient cloudwatchClient;


    @Inject
    public ReconciliationMetricReportingServiceImpl(
            JobArguments jobArguments,
            CloudwatchClient cloudwatchClient
    ) {
        this.jobArguments = jobArguments;
        this.cloudwatchClient = cloudwatchClient;
    }

    @Override
    public void reportMetrics(DataReconciliationResults dataReconciliationResults) {
        String inputDomain = jobArguments.getConfigKey();
        cloudwatchClient.putMetrics(jobArguments.getReconciliationCloudwatchMetricsNamespace(), dataReconciliationResults.toCloudwatchMetricData(inputDomain));
    }
}