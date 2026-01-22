package uk.gov.justice.digital.service.metrics;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
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

/**
 * Context for accumulating metrics for reporting to CloudWatch and then flushing in batches.
 * Note that this class is not thread-safe and should be used from a single thread.
 *
 * Note that Each PutMetricData request is limited to 1 MB in size for HTTP POST requests.
 * You can send a payload compressed by gzip. Each request is also limited to no more than 1000 different
 * metrics (across both the MetricData and EntityMetricData properties).
 * See <a href="https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html">the API docs</a>
 */
public class CloudwatchBatchMetrics implements BatchMetrics {

    private static final Logger logger = LoggerFactory.getLogger(CloudwatchBatchMetrics.class);

    private final JobArguments jobArguments;
    private final JobProperties jobProperties;
    private final CloudwatchClient cloudwatchClient;

    private final Set<MetricDatum> bufferedMetrics = new HashSet<>();

    public CloudwatchBatchMetrics(
            JobArguments jobArguments,
            JobProperties jobProperties,
            CloudwatchClient cloudwatchClient
    ) {
        this.jobArguments = jobArguments;
        this.jobProperties = jobProperties;
        this.cloudwatchClient = cloudwatchClient;
    }

    @Override
    public void bufferViolationCount(long count) {
        String jobName = jobProperties.getSparkJobName();
        bufferMetricWithSingleDimension(MetricName.GLUE_JOB_VIOLATION_COUNT, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void bufferDataReconciliationResults(DataReconciliationResults dataReconciliationResults) {
        String inputDomain = jobArguments.getConfigKey();
        double numChecksFailing = dataReconciliationResults.numReconciliationChecksFailing();
        bufferMetricWithSingleDimension(MetricName.FAILED_RECONCILIATION_CHECKS, DimensionName.INPUT_DOMAIN, inputDomain, Count, numChecksFailing);
    }

    @Override
    public void bufferStreamingThroughputInput(Dataset<Row> inputDf) {
        String jobName = jobProperties.getSparkJobName();
        long count = inputDf.count();
        bufferMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_INPUT, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void bufferStreamingThroughputWrittenToStructured(Dataset<Row> structuredDf) {
        String jobName = jobProperties.getSparkJobName();
        long count = structuredDf.count();
        bufferMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_STRUCTURED, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void bufferStreamingThroughputWrittenToCurated(Dataset<Row> curatedDf) {
        String jobName = jobProperties.getSparkJobName();
        long count = curatedDf.count();
        bufferMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_CURATED, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void bufferStreamingMicroBatchTimeTaken(long timeTakenMs) {
        String jobName = jobProperties.getSparkJobName();
        bufferMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_MICROBATCH_TIME, DimensionName.JOB_NAME, jobName, Milliseconds, timeTakenMs);
    }

    @Override
    public void close() {
        String metricNamespace = jobArguments.getCloudwatchMetricsNamespace();
        cloudwatchClient.putMetrics(metricNamespace, bufferedMetrics);
        // Clear the buffer just in case someone goes against the javadoc and keeps a reference to the object around in the future
        bufferedMetrics.clear();
        logger.debug("Flushed metrics for namespace {}", metricNamespace);
    }

    private void bufferMetricWithSingleDimension(MetricName metricName, DimensionName metricDimensionName, String dimensionValue, StandardUnit unit, double value) {

        logger.debug("Preparing {} metric with value {}", metricName, value);
        bufferedMetrics.add(
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
        logger.debug("Finished preparing {} metric with value {}", metricName, value);
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
