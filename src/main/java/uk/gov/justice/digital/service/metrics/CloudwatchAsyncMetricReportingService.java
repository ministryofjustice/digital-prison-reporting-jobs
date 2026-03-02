package uk.gov.justice.digital.service.metrics;

import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;
import uk.gov.justice.digital.client.cloudwatch.DefaultCloudwatchClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;
import uk.gov.justice.digital.service.shutdownhooks.ShutdownHookRegistrar;
import uk.gov.justice.digital.service.shutdownhooks.ShutdownHooks;

import javax.annotation.PreDestroy;
import javax.inject.Named;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
    // We expect that the buffer should never grow beyond a certain size
    private static final int MAX_EXPECTED_METRICS_IN_BUFFER = 1000;

    private final DefaultCloudwatchClient cloudwatchClient;
    private final Clock clock;
    private final ScheduledExecutorService schedulerService;
    private final ShutdownHookRegistrar shutdownHookRegistrar;

    private final String metricNamespace;
    private final String jobName;
    private final long flushPeriodSeconds;
    private final long shutdownflushTimeoutSeconds;

    // This lock MUST guard all access to the buffered metrics collection for correct metric publishing
    private final Object lock = new Object();
    private List<MetricDatum> bufferedMetrics = new ArrayList<>();

    private volatile ScheduledFuture<?> scheduledMetricFlushTask;

    @Inject
    public CloudwatchAsyncMetricReportingService(
            JobArguments jobArguments,
            JobProperties jobProperties,
            DefaultCloudwatchClient cloudwatchClient,
            Clock clock,
            @Named("metricsFlusher")
            ScheduledExecutorService schedulerService,
            ShutdownHookRegistrar shutdownHookRegistrar
    ) {
        this.cloudwatchClient = cloudwatchClient;
        this.clock = clock;
        this.schedulerService = schedulerService;
        this.shutdownHookRegistrar = shutdownHookRegistrar;
        this.metricNamespace = jobArguments.getCloudwatchMetricsNamespace();
        this.jobName = jobProperties.getSparkJobName();
        this.flushPeriodSeconds = jobArguments.getCloudwatchMetricsReportingPeriodSeconds();
        this.shutdownflushTimeoutSeconds = jobArguments.getCloudwatchMetricsShutdownFlushTimeoutSeconds();
    }

    @PostConstruct
    void start() {
        logger.info("Starting Cloudwatch metric background flush task, flushing every {} seconds", flushPeriodSeconds);
        this.scheduledMetricFlushTask = schedulerService.scheduleAtFixedRate(this::flush, 0, flushPeriodSeconds, TimeUnit.SECONDS);
        this.shutdownHookRegistrar.registerShutdownHook(
                ShutdownHooks.withTimeout("best-effort-shutdown-flush", Duration.ofSeconds(shutdownflushTimeoutSeconds), () -> {
                    ScheduledFuture<?> flushTask = this.scheduledMetricFlushTask;
                    if (flushTask != null) {
                        logger.info("Cancelling scheduled metric flush task");
                        flushTask.cancel(true);
                        logger.info("Flushing metrics before shutdown");
                        flush();
                    }
                }));
    }


    @Override
    public void reportViolationCount(long count) {
        MetricDatum metricDatum = buildValueMetricWithJobNameDimension(MetricName.GLUE_JOB_VIOLATION_COUNT, StandardUnit.COUNT, count);
        bufferMetric(metricDatum);
    }

    @Override
    public void reportDataReconciliationResults(DataReconciliationResults dataReconciliationResults) {
        double numChecksFailing = dataReconciliationResults.numReconciliationChecksFailing();
        MetricDatum metricDatum = buildValueMetricWithJobNameDimension(MetricName.FAILED_RECONCILIATION_CHECKS, StandardUnit.COUNT, numChecksFailing);
        bufferMetric(metricDatum);
    }

    @Override
    public void reportStreamingThroughputInput(long count) {
        MetricDatum metricDatum = buildValueMetricWithJobNameDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_INPUT, StandardUnit.COUNT, count);
        bufferMetric(metricDatum);
    }

    @Override
    public void reportStreamingThroughputWrittenToStructured(long count) {
        MetricDatum metricDatum = buildValueMetricWithJobNameDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_STRUCTURED, StandardUnit.COUNT, count);
        bufferMetric(metricDatum);
    }

    @Override
    public void reportStreamingThroughputWrittenToCurated(long count) {
        MetricDatum metricDatum = buildValueMetricWithJobNameDimension(MetricName.GLUE_JOB_STREAMING_THROUGHPUT_CURATED, StandardUnit.COUNT, count);
        bufferMetric(metricDatum);
    }

    @Override
    public void reportStreamingMicroBatchTimeTaken(long timeTakenMs) {
        MetricDatum metricDatum = buildValueMetricWithJobNameDimension(MetricName.GLUE_JOB_STREAMING_MICROBATCH_TIME, StandardUnit.MILLISECONDS, timeTakenMs);
        bufferMetric(metricDatum);
    }

    @Override
    public void reportStreamingLatencyDmsToCurated(LatencyStatistics latencyStatistics) {
        Optional<StatisticSet> statisticSet = convertToStatisticSet(latencyStatistics);
        statisticSet.ifPresent(stat -> {
            MetricDatum metricDatum = buildStatisticMetricWithJobNameDimension(MetricName.GLUE_JOB_STREAMING_LATENCY_DMS_TO_CURATED, StandardUnit.MILLISECONDS, stat);
            bufferMetric(metricDatum);
        });
    }

    @Override
    public void reportBatchJobTimeTaken(long timeTakenMs) {
        MetricDatum metricDatum = buildValueMetricWithJobNameDimension(MetricName.GLUE_JOB_BATCH_TIME_TAKEN, StandardUnit.MILLISECONDS, timeTakenMs);
        bufferMetric(metricDatum);
    }

    /**
     * Flush any leftover metrics to Cloudwatch before Micronaut shuts down
     */
    @PreDestroy
    public void close() {
        logger.info("PreDestroy: Cancelling scheduled flush task");
        ScheduledFuture<?> flushTask = this.scheduledMetricFlushTask;
        if (flushTask != null) {
            flushTask.cancel(false);
        }
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
        } catch (SdkClientException e) {
            // Metrics are published on a best effort basis based on the underlying client's retry policy.
            // We don't want to fail a job due to the Cloudwatch API being unavailable.
            logger.error("Failed to report metrics to CloudWatch", e);
        }
    }

    private MetricDatum buildValueMetricWithJobNameDimension(MetricName metricName, StandardUnit unit, double value) {
        return MetricDatum
                .builder()
                .metricName(metricName.toString())
                .unit(unit)
                .dimensions(
                        Dimension.builder()
                                .name(DimensionName.JOB_NAME.toString())
                                .value(jobName)
                                .build()
                )
                .timestamp(clock.instant())
                .value(value)
                .build();
    }

    private MetricDatum buildStatisticMetricWithJobNameDimension(MetricName metricName, StandardUnit unit, StatisticSet statistics) {
        return MetricDatum
                .builder()
                .metricName(metricName.toString())
                .unit(unit)
                .dimensions(
                        Dimension.builder()
                                .name(DimensionName.JOB_NAME.toString())
                                .value(jobName)
                                .build()
                )
                .timestamp(clock.instant())
                .statisticValues(statistics)
                .build();
    }

    private void bufferMetric(MetricDatum metricDatum) {
        logger.debug("Reporting {} metric for namespace {} with value {}", metricDatum.metricName(), metricNamespace, metricDatum.value());
        int bufferedCount;
        synchronized (lock) {
            bufferedMetrics.add(
                    metricDatum
            );
            bufferedCount = bufferedMetrics.size();
        }

        logger.debug("Finished batching {} metric for namespace {} with value {}", metricDatum.metricName(), metricNamespace, metricDatum.value());

        if (bufferedCount > MAX_EXPECTED_METRICS_IN_BUFFER) {
            logger.error("There are {} buffered metrics. This could be an indicator that metrics are not being flushed correctly", bufferedCount);
        }
    }

    private static Optional<StatisticSet> convertToStatisticSet(LatencyStatistics latencyStatistics) {
        if (LatencyStatistics.isEmpty(latencyStatistics)) {
            return Optional.empty();
        }
        StatisticSet statisticSet = StatisticSet.builder()
                .maximum((double) latencyStatistics.getMaximum())
                .minimum((double) latencyStatistics.getMinimum())
                .sum((double) latencyStatistics.getSum())
                .sampleCount((double) latencyStatistics.getTotalCount())
                .build();
        return Optional.of(statisticSet);
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
        GLUE_JOB_STREAMING_MICROBATCH_TIME("GlueJobStreamingMicroBatchTime"),
        GLUE_JOB_STREAMING_LATENCY_DMS_TO_CURATED("GlueJobStreamingLatencyDmsToCurated"),
        GLUE_JOB_BATCH_TIME_TAKEN("GlueJobBatchTimeTaken");

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
