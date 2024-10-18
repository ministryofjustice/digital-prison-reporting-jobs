package uk.gov.justice.digital.service.datareconciliation;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.cloudwatch.CloudwatchClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import java.util.HashSet;
import java.util.Set;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;

@Singleton
@Requires(property = "dpr.reconciliation.report.results.to.cloudwatch")
public class ReconciliationMetricReportingServiceImpl implements ReconciliationMetricReportingService {

    private static final Logger logger = LoggerFactory.getLogger(ReconciliationMetricReportingServiceImpl.class);

    private static final String FAILED_RECONCILIATION_CHECKS_METRIC_NAME = "FailedReconciliationChecks";

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
        String metricNamespace = jobArguments.getReconciliationCloudwatchMetricsNamespace();
        String inputDomain = jobArguments.getConfigKey();
        double numChecksFailing = dataReconciliationResults.numReconciliationChecksFailing();
        logger.debug("Reporting {} metric to namespace {} for domain {} with value {}",
                FAILED_RECONCILIATION_CHECKS_METRIC_NAME, metricNamespace, inputDomain, numChecksFailing);
        Set<MetricDatum> metrics = new HashSet<>();
        metrics.add(
                new MetricDatum()
                        .withMetricName(FAILED_RECONCILIATION_CHECKS_METRIC_NAME)
                        .withUnit(Count)
                        .withDimensions(
                                new Dimension()
                                        .withName("InputDomain")
                                        .withValue(inputDomain)
                        )
                        .withValue(numChecksFailing)
        );
        cloudwatchClient.putMetrics(metricNamespace, metrics);
        logger.debug("Finished reporting {} metric to namespace {} for domain {} with value {}",
                FAILED_RECONCILIATION_CHECKS_METRIC_NAME, metricNamespace, inputDomain, numChecksFailing);
    }
}