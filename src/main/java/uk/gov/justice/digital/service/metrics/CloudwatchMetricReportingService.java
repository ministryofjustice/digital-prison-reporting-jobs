package uk.gov.justice.digital.service.metrics;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.cloudwatch.CloudwatchClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import java.util.HashSet;
import java.util.Set;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;
import static com.amazonaws.services.cloudwatch.model.StandardUnit.Milliseconds;
import static uk.gov.justice.digital.config.JobArguments.REPORT_METRICS_TO_CLOUDWATCH;

@Singleton
@Requires(property = REPORT_METRICS_TO_CLOUDWATCH)
public class CloudwatchMetricReportingService implements MetricReportingService {

    private static final Logger logger = LoggerFactory.getLogger(CloudwatchMetricReportingService.class);

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
        String jobName = jobProperties.getSparkJobName();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_VIOLATION_COUNT, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void reportDataReconciliationResults(DataReconciliationResults dataReconciliationResults) {
        String inputDomain = jobArguments.getConfigKey();
        double numChecksFailing = dataReconciliationResults.numReconciliationChecksFailing();
        putMetricWithSingleDimension(MetricName.FAILED_RECONCILIATION_CHECKS, DimensionName.INPUT_DOMAIN, inputDomain, Count, numChecksFailing);
    }

    @Override
    public void reportStreamingThroughputInput(Dataset<Row> inputDf) {
        String jobName = jobProperties.getSparkJobName();
        long count = inputDf.count();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_INPUT, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void reportStreamingThroughputWrittenToStructured(Dataset<Row> structuredDf) {
        String jobName = jobProperties.getSparkJobName();
        long count = structuredDf.count();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_STRUCTURED, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void reportStreamingThroughputWrittenToCurated(Dataset<Row> curatedDf) {
        String jobName = jobProperties.getSparkJobName();
        long count = curatedDf.count();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_CURATED, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void reportStreamingMicroBatchTimeTaken(long timeTakenMs) {
        String jobName = jobProperties.getSparkJobName();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_MICROBATCH_TIME, DimensionName.JOB_NAME, jobName, Milliseconds, timeTakenMs);
    }

    private void putMetricWithSingleDimension(MetricName metricName, DimensionName metricDimensionName, String dimensionValue, StandardUnit unit, double value) {
        String metricNamespace = jobArguments.getCloudwatchMetricsNamespace();
        logger.debug("Reporting {} metric to namespace {} with value {}", metricName, metricNamespace, value);

        // Currently, this class makes a putMetrics call to CloudWatch for every metric.
        // DHS-594 will batch up calls to save API call costs in jobs that report more than one metric.
        Set<MetricDatum> metrics = new HashSet<>();
        metrics.add(
                new MetricDatum()
                        .withMetricName(metricName.toString())
                        .withUnit(unit)
                        .withDimensions(
                                new Dimension()
                                        .withName(metricDimensionName.toString())
                                        .withValue(dimensionValue)
                        )
                        .withValue(value)
        );
        cloudwatchClient.putMetrics(metricNamespace, metrics);
        logger.debug("Finished reporting {} metric to namespace {} with value {}", metricName, metricNamespace, value);
    }

    /**
     * Enum representing different types of metrics to be reported to CloudWatch.
     */
    private enum MetricName {

        FAILED_RECONCILIATION_CHECKS("FailedReconciliationChecks"),
        GLUE_JOB_VIOLATION_COUNT("GlueJobViolationCount"),
        GLUE_JOB_STREAMING_THROUGHPUT_INPUT("GlueJobStreamingThroughputInputCount"),
        GLUE_JOB_STREAMING_THROUGHPUT_STRUCTURED("GlueJobStreamingThroughputStructuredCount"),
        GLUE_JOB_STREAMING_THROUGHPUT_CURATED("GlueJobStreamingThroughputCuratedCount"),
        GLUE_JOB_STREAMING_MICROBATCH_TIME("GlueJobStreamingMicroBatchTime");

        private final String value;

        MetricName(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /**
     * Enum representing different dimensions for metrics reported to CloudWatch.
     */
    private enum DimensionName {
        INPUT_DOMAIN("InputDomain"),
        JOB_NAME("JobName");

        private final String value;

        DimensionName(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
