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

import javax.annotation.PreDestroy;
import javax.inject.Named;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;
import static com.amazonaws.services.cloudwatch.model.StandardUnit.Milliseconds;
import static uk.gov.justice.digital.config.JobArguments.REPORT_METRICS_TO_CLOUDWATCH;

/**
 * Buffers cloudwatch metrics locally before sending them in batches using a background thread to the Cloudwatch API
 * to save API call costs. When shutdown gracefully, we flush any remaining metrics to Cloudwatch.
 * This class is a singleton, and so it will buffer metrics for all StreamingQueries (source input tables) across a streaming app.
 * This means that metrics for different streaming queries can be sent in one batch and metrics for a given streaming
 * query microbatch can be interleaved across different batches. This means that you should always set the timestamp field
 * on any MetricDatum objects reported from this class.
 */
@Singleton
@Requires(property = REPORT_METRICS_TO_CLOUDWATCH)
public class CloudwatchAsyncMetricReportingService implements MetricReportingService {

    private static final Logger logger = LoggerFactory.getLogger(CloudwatchAsyncMetricReportingService.class);

    private final CloudwatchClient cloudwatchClient;
    private final Clock clock;
    private final String metricNamespace;
    private final String jobName;
    private final String inputDomain;

    // This lock MUST guard all access to the buffered metrics collection for correct metric publishing
    private final Object lock = new Object();
    private List<MetricDatum> bufferedMetrics = new ArrayList<>();

    private final ScheduledFuture<?> scheduledMetricFlushTask;

    @Inject
    public CloudwatchAsyncMetricReportingService(
            JobArguments jobArguments,
            JobProperties jobProperties,
            CloudwatchClient cloudwatchClient,
            Clock clock,
            @Named("metricsFlusher")
            ScheduledExecutorService schedulerService
    ) {
        this.cloudwatchClient = cloudwatchClient;
        this.clock = clock;
        this.metricNamespace = jobArguments.getCloudwatchMetricsNamespace();
        this.jobName = jobProperties.getSparkJobName();
        this.inputDomain = jobArguments.getConfigKey();
        // Flush metrics to Cloudwatch on the configured schedule
        long periodSeconds = jobArguments.getCloudwatchMetricsReportingPeriodSeconds();
        logger.info("Starting Cloudwatch metric background flush task, flushing every {} seconds", periodSeconds);
        this.scheduledMetricFlushTask = schedulerService.scheduleAtFixedRate(this::flush, 0, periodSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void reportViolationCount(long count) {
        putMetricWithSingleDimension(MetricName.GLUE_JOB_VIOLATION_COUNT, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void reportDataReconciliationResults(DataReconciliationResults dataReconciliationResults) {
        double numChecksFailing = dataReconciliationResults.numReconciliationChecksFailing();
        putMetricWithSingleDimension(MetricName.FAILED_RECONCILIATION_CHECKS, DimensionName.INPUT_DOMAIN, inputDomain, Count, numChecksFailing);
    }

    @Override
    public void reportStreamingThroughputInput(Dataset<Row> inputDf) {
        long count = inputDf.count();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_INPUT, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void reportStreamingThroughputWrittenToStructured(Dataset<Row> structuredDf) {
        long count = structuredDf.count();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_STRUCTURED, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void reportStreamingThroughputWrittenToCurated(Dataset<Row> curatedDf) {
        long count = curatedDf.count();
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_CURATED, DimensionName.JOB_NAME, jobName, Count, count);
    }

    @Override
    public void reportStreamingMicroBatchTimeTaken(long timeTakenMs) {
        putMetricWithSingleDimension(MetricName.GLUE_JOB_STREAMING_MICROBATCH_TIME, DimensionName.JOB_NAME, jobName, Milliseconds, timeTakenMs);
    }

    /**
     * Flush any leftover metrics to Cloudwatch before Micronaut shuts down
     */
    @PreDestroy
    public void close() {
        logger.info("PreDestroy: Cancelling scheduled flush task");
        scheduledMetricFlushTask.cancel(false);
        logger.info("PreDestroy: Flushing metrics to cloudwatch");
        flush();
        logger.info("PreDestroy: Finished flushing metrics to cloudwatch");
    }

    /**
     * Flush any buffered metrics to Cloudwatch.
     */
    void flush() {
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
            // Metrics are published on a best effort basis based on the underlying client's retry policy.
            // We don't want to fail a job due to the Cloudwatch API being unavailable.
            logger.error("Failed to report metrics to CloudWatch", e);
        }
    }

    private void putMetricWithSingleDimension(MetricName metricName, DimensionName metricDimensionName, String dimensionValue, StandardUnit unit, double value) {
        logger.debug("Reporting {} metric for namespace {} with value {}", metricName, metricNamespace, value);
        MetricDatum metricDatum = new MetricDatum()
                .withMetricName(metricName.toString())
                .withUnit(unit)
                .withDimensions(
                        new Dimension()
                                .withName(metricDimensionName.toString())
                                .withValue(dimensionValue)
                )
                .withTimestamp(Date.from(clock.instant()))
                .withValue(value);

        synchronized (lock) {
            bufferedMetrics.add(
                    metricDatum
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
