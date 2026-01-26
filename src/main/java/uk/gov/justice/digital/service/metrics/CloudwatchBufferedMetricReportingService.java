package uk.gov.justice.digital.service.metrics;

import com.amazonaws.AmazonClientException;
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

import java.time.Clock;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;
import static com.amazonaws.services.cloudwatch.model.StandardUnit.Milliseconds;
import static uk.gov.justice.digital.config.JobArguments.REPORT_METRICS_TO_CLOUDWATCH;

/**
 * Buffers cloudwatch metrics locally before sending them in batches to the Cloudwatch API to save API call costs.
 * This class is a singleton, and so it will buffer metrics for all StreamingQueries (source input tables) across a streaming app.
 * This means that metrics for different streaming queries can be sent in one batch and metrics for a given streaming
 * query microbatch can be interleaved across different batches. This means that you should always set the timestamp field
 * on any MetricDatum objects reported from this class.
 */
@Singleton
@Requires(property = REPORT_METRICS_TO_CLOUDWATCH)
public class CloudwatchBufferedMetricReportingService implements BufferedMetricReportingService {

    private static final Logger logger = LoggerFactory.getLogger(CloudwatchBufferedMetricReportingService.class);

    private final CloudwatchClient cloudwatchClient;
    private final Clock clock;
    private final String metricNamespace;
    private final String jobName;
    private final String inputDomain;

    // This lock MUST guard all access to the buffered metrics collection for correct metric publishing
    private final Object lock = new Object();
    private List<MetricDatum> bufferedMetrics = new ArrayList<>();


    @Inject
    public CloudwatchBufferedMetricReportingService(
            JobArguments jobArguments,
            JobProperties jobProperties,
            CloudwatchClient cloudwatchClient,
            Clock clock
    ) {
        this.cloudwatchClient = cloudwatchClient;
        this.clock = clock;
        this.metricNamespace = jobArguments.getCloudwatchMetricsNamespace();
        this.jobName = jobProperties.getSparkJobName();
        this.inputDomain = jobArguments.getConfigKey();
    }

    @Override
    public void bufferViolationCount(long count) {
        putMetricWithSingleDimension(MetricName.GLUE_JOB_VIOLATION_COUNT, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void bufferDataReconciliationResults(DataReconciliationResults dataReconciliationResults) {
        double numChecksFailing = dataReconciliationResults.numReconciliationChecksFailing();
        putMetricWithSingleDimension(MetricName.FAILED_RECONCILIATION_CHECKS, DimensionName.INPUT_DOMAIN, inputDomain, Count, numChecksFailing);
    }

    @Override
    public void bufferStreamingThroughputInput(Dataset<Row> inputDf) {
        long count = inputDf.count();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_INPUT, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void bufferStreamingThroughputWrittenToStructured(Dataset<Row> structuredDf) {
        long count = structuredDf.count();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_STRUCTURED, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void bufferStreamingThroughputWrittenToCurated(Dataset<Row> curatedDf) {
        long count = curatedDf.count();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_CURATED, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void bufferStreamingMicroBatchTimeTaken(long timeTakenMs) {
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_MICROBATCH_TIME, DimensionName.JOB_NAME, jobName, Milliseconds, timeTakenMs);
    }

    @Override
    public void flushAllBufferedMetrics() {
        List<MetricDatum> copiedMetricData;
        synchronized (lock) {
            if (bufferedMetrics.isEmpty()) {
                return;
            }
            copiedMetricData = bufferedMetrics;
            // Clear the buffer for the next batch
            bufferedMetrics = new ArrayList<>();
        }

        // We don't want to increase contention by serialising the network call under the lock
        try {
            cloudwatchClient.putMetrics(metricNamespace, copiedMetricData);
        } catch (AmazonClientException e) {
            // Metrics are published on a best effort basis. We don't want
            // to fail a job due to the Cloudwatch API being unavailable.
            logger.warn("Failed to report metrics to CloudWatch", e);
        }
    }

    private void putMetricWithSingleDimension(MetricName metricName, DimensionName metricDimensionName, String dimensionValue, StandardUnit unit, double value) {
        logger.debug("Reporting {} metric for namespace {} with value {}", metricName, metricNamespace, value);

        synchronized (lock) {
            bufferedMetrics.add(
                    new MetricDatum()
                            .withMetricName(metricName.toString())
                            .withUnit(unit)
                            .withDimensions(
                                    new Dimension()
                                            .withName(metricDimensionName.toString())
                                            .withValue(dimensionValue)
                            )
                            .withTimestamp(Date.from(clock.instant()))
                            .withValue(value)
            );
        }

        logger.debug("Finished batching {} metric for namespace {} with value {}", metricName, metricNamespace, value);
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
