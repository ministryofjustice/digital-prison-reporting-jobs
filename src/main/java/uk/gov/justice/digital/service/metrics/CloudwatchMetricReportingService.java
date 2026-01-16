package uk.gov.justice.digital.service.metrics;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.cloudwatch.CloudwatchClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import java.util.HashSet;
import java.util.Set;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;
import static uk.gov.justice.digital.config.JobArguments.REPORT_METRICS_TO_CLOUDWATCH;

@Singleton
@Requires(property = REPORT_METRICS_TO_CLOUDWATCH)
public class CloudwatchMetricReportingService implements MetricReportingService {

    private static final Logger logger = LoggerFactory.getLogger(CloudwatchMetricReportingService.class);

    // Metric names
    private static final String FAILED_RECONCILIATION_CHECKS_METRIC_NAME = "FailedReconciliationChecks";
    private static final String GLUE_JOB_VIOLATION_COUNT_METRIC_NAME = "GlueJobViolationCount";
    // Dimension names
    private static final String INPUT_DOMAIN_DIMENSION = "InputDomain";
    private static final String JOB_NAME_DIMENSION = "JobName";

    private final JobArguments jobArguments;
    private final JobProperties jobProperties;
    private final CloudwatchClient cloudwatchClient;


    @Inject
    public CloudwatchMetricReportingService(
            JobArguments jobArguments,
            JobProperties jobProperties,
            CloudwatchClient cloudwatchClient
    ) {
        this.jobArguments = jobArguments;
        this.jobProperties = jobProperties;
        this.cloudwatchClient = cloudwatchClient;
    }

    @Override
    public void reportViolationCount(long count) {
        String metricNamespace = jobArguments.getCloudwatchMetricsNamespace();
        String jobName = jobProperties.getSparkJobName();
        logger.debug("Reporting {} metric to namespace {} with value {}",
                GLUE_JOB_VIOLATION_COUNT_METRIC_NAME, metricNamespace, count);

        Set<MetricDatum> metrics = new HashSet<>();
        metrics.add(
                new MetricDatum()
                        .withMetricName(GLUE_JOB_VIOLATION_COUNT_METRIC_NAME)
                        .withUnit(Count)
                        .withDimensions(
                                new Dimension()
                                        .withName(JOB_NAME_DIMENSION)
                                        .withValue(jobName)
                        )
                        .withValue((double)count)
        );
        cloudwatchClient.putMetrics(metricNamespace, metrics);
        logger.debug("Finished reporting {} metric to namespace {} with value {}",
                GLUE_JOB_VIOLATION_COUNT_METRIC_NAME, metricNamespace, count);
    }

    @Override
    public void reportDataReconciliationResults(DataReconciliationResults dataReconciliationResults) {
        String metricNamespace = jobArguments.getCloudwatchMetricsNamespace();
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
                                        .withName(INPUT_DOMAIN_DIMENSION)
                                        .withValue(inputDomain)
                        )
                        .withValue(numChecksFailing)
        );
        cloudwatchClient.putMetrics(metricNamespace, metrics);
        logger.debug("Finished reporting {} metric to namespace {} for domain {} with value {}",
                FAILED_RECONCILIATION_CHECKS_METRIC_NAME, metricNamespace, inputDomain, numChecksFailing);
    }
}
