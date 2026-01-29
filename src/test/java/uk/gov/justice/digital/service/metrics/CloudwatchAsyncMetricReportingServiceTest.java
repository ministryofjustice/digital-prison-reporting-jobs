package uk.gov.justice.digital.service.metrics;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.cloudwatch.CloudwatchClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;
import static com.amazonaws.services.cloudwatch.model.StandardUnit.Milliseconds;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CloudwatchAsyncMetricReportingServiceTest {

    private static final String DOMAIN = "some domain";
    private static final String JOB = "some job";
    private static final String NAMESPACE = "SomeNamespace";
    private static final Instant timestamp = Instant.ofEpochMilli(123456789L);
    @Mock
    private JobArguments jobArguments;
    @Mock
    private JobProperties jobProperties;
    @Mock
    private CloudwatchClient cloudwatchClient;
    @Mock
    private Clock clock;
    @Mock
    private ScheduledExecutorService schedulerService;
    @Mock
    private ScheduledFuture scheduledFlushTask;
    @Captor
    private ArgumentCaptor<Collection<MetricDatum>> metricDatumCaptor;
    @Mock
    private Dataset<Row> mockDf;

    private CloudwatchAsyncMetricReportingService underTest;

    @BeforeEach
    void setUp() {
        when(jobProperties.getSparkJobName()).thenReturn(JOB);
        when(jobArguments.getCloudwatchMetricsNamespace()).thenReturn(NAMESPACE);
        when(jobArguments.getConfigKey()).thenReturn(DOMAIN);
        when(schedulerService.scheduleAtFixedRate(any(), anyLong(), anyLong(), any())).thenReturn(scheduledFlushTask);
        underTest = new CloudwatchAsyncMetricReportingService(jobArguments, jobProperties, cloudwatchClient, clock, schedulerService);
        verify(schedulerService, times(1)).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
    }

    @Test
    void reportViolationCountShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);

        underTest.reportViolationCount(10L);
        underTest.flush();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobViolationCount", datum.getMetricName());
        assertEquals(10L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        assertEquals(timestamp, datum.getTimestamp().toInstant());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void reportDataReconciliationResultsShouldPutMetrics() {
        DataReconciliationResults dataReconciliationResults = mock(DataReconciliationResults.class);

        when(clock.instant()).thenReturn(timestamp);
        when(dataReconciliationResults.numReconciliationChecksFailing()).thenReturn(2L);

        underTest.reportDataReconciliationResults(dataReconciliationResults);
        underTest.flush();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("FailedReconciliationChecks", datum.getMetricName());
        assertEquals(2L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        assertEquals(timestamp, datum.getTimestamp().toInstant());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("InputDomain", dimension.getName());
        assertEquals(DOMAIN, dimension.getValue());
    }

    @Test
    void reportStreamingThroughputInputShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);
        long expectedCount = 100L;

        underTest.reportStreamingThroughputInput(expectedCount);
        underTest.flush();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingThroughputInputCount", datum.getMetricName());
        assertEquals(expectedCount, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        assertEquals(timestamp, datum.getTimestamp().toInstant());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void reportStreamingThroughputWrittenToStructuredShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);
        long expectedCount = 100L;
        underTest.reportStreamingThroughputWrittenToStructured(expectedCount);
        underTest.flush();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingThroughputStructuredCount", datum.getMetricName());
        assertEquals(expectedCount, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        assertEquals(timestamp, datum.getTimestamp().toInstant());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void reportStreamingThroughputWrittenToCuratedShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);

        long expectedCount = 100L;

        underTest.reportStreamingThroughputWrittenToCurated(expectedCount);
        underTest.flush();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingThroughputCuratedCount", datum.getMetricName());
        assertEquals(expectedCount, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        assertEquals(timestamp, datum.getTimestamp().toInstant());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void reportStreamingMicroBatchTimeTakenShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);

        underTest.reportStreamingMicroBatchTimeTaken(1000L);
        underTest.flush();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingMicroBatchTime", datum.getMetricName());
        assertEquals(1000L, datum.getValue());
        assertEquals(Milliseconds.toString(), datum.getUnit());
        assertEquals(timestamp, datum.getTimestamp().toInstant());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void reportStreamingLatencyDmsToCuratedShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);

        LatencyStatistics latencyStatistics = new LatencyStatistics(10L, 1000L, 1010L, 2L);

        underTest.reportStreamingLatencyDmsToCurated(latencyStatistics);
        underTest.flush();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingLatencyDmsToCurated", datum.getMetricName());
        StatisticSet actualStatisticSet = datum.getStatisticValues();
        assertEquals(10L, actualStatisticSet.getMinimum());
        assertEquals(1000L, actualStatisticSet.getMaximum());
        assertEquals(1010L, actualStatisticSet.getSum());
        assertEquals(2L, actualStatisticSet.getSampleCount());
        assertEquals(Milliseconds.toString(), datum.getUnit());
        assertEquals(timestamp, datum.getTimestamp().toInstant());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void reportStreamingLatencyDmsToCuratedShouldDoNothingForEmptyStatistics() {
        underTest.reportStreamingLatencyDmsToCurated(LatencyStatistics.EMPTY);
        underTest.flush();

        verifyNoInteractions(cloudwatchClient);
    }

    @Test
    void flushShouldSendMetrics() {
        when(clock.instant()).thenReturn(timestamp);

        underTest.reportStreamingThroughputWrittenToCurated(100L);
        underTest.flush();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());
    }

    @Test
    void flushShouldSendMultipleMetrics() {
        when(clock.instant()).thenReturn(timestamp);

        underTest.reportStreamingThroughputWrittenToCurated(100L);
        underTest.flush();

        underTest.reportStreamingMicroBatchTimeTaken(1000L);
        underTest.flush();

        verify(cloudwatchClient, times(2)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());
    }

    @Test
    void flushShouldSendMultipleMetricsInOneCall() {
        when(clock.instant()).thenReturn(timestamp);

        underTest.reportStreamingThroughputWrittenToCurated(100L);
        underTest.reportStreamingMicroBatchTimeTaken(1000L);
        underTest.flush();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();
        assertEquals(2, sentMetrics.size());
    }

    @Test
    void flushingEmptyBufferShouldDoNothing() {
        underTest.flush();
        verifyNoInteractions(cloudwatchClient);
    }

    @Test
    void flushShouldContinueWhenCloudwatchThrows() {
        when(clock.instant()).thenReturn(timestamp);
        doThrow(new AmazonClientException("Test Exception")).when(cloudwatchClient).putMetrics(anyString(), anyList());
        underTest.reportStreamingMicroBatchTimeTaken(1000L);
        assertDoesNotThrow(() -> underTest.flush());
    }

    @Test
    void closeShouldSendMetrics() {
        when(clock.instant()).thenReturn(timestamp);

        underTest.reportStreamingThroughputWrittenToCurated(100L);
        underTest.close();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());
    }

    @Test
    void closeShouldCancelScheduledFlush() {
        when(clock.instant()).thenReturn(timestamp);

        underTest.reportStreamingThroughputWrittenToCurated(100L);
        underTest.close();

        verify(scheduledFlushTask, times(1)).cancel(anyBoolean());
    }

    @Test
    void manyWritersSingleFlushShouldPublishCorrectly() throws ExecutionException, InterruptedException, TimeoutException {
        when(clock.instant()).thenReturn(timestamp);

        int timeoutSeconds = 5;
        int numThreads = 10;
        int metricsPerThread = 1000;
        int totalMetrics = numThreads * metricsPerThread;

        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        CyclicBarrier startBarrier = new CyclicBarrier(numThreads);
        List<Future<?>> futures = new ArrayList<>();

        // Report metrics to the buffer from many threads
        for (int t = 0; t < numThreads; t++) {
            futures.add(pool.submit(() -> {
                try {
                    // Wait until all tasks are ready before starting
                    startBarrier.await(timeoutSeconds, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    fail();
                }
                for (int i = 0; i < metricsPerThread; i++) {
                    // Report the metric multiple times from this thread
                    underTest.reportStreamingMicroBatchTimeTaken(i);
                }
            }));
        }

        // Wait for all tasks to complete
        for (Future<?> f : futures) {
            f.get(timeoutSeconds, TimeUnit.SECONDS);
        }
        // Flush once from the main thread
        underTest.flush();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();
        assertEquals(totalMetrics, sentMetrics.size());
    }

    @SuppressWarnings("java:S2925")
    @Test
    void manyWritersManyFlushersShouldPublishCorrectly() throws ExecutionException, InterruptedException, TimeoutException {
        when(clock.instant()).thenReturn(timestamp);

        int timeoutSeconds = 5;
        int numThreads = 10;
        int metricsPerThread = 10;
        int totalMetrics = numThreads * metricsPerThread;

        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        CyclicBarrier startBarrier = new CyclicBarrier(numThreads);
        List<Future<?>> futures = new ArrayList<>();

        // Report metrics to the buffer and flush from many threads
        for (int t = 0; t < numThreads; t++) {
            futures.add(pool.submit(() -> {
                try {
                    // Wait until all tasks are ready before starting
                    startBarrier.await(timeoutSeconds, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    fail();
                }
                for (int i = 0; i < metricsPerThread; i++) {
                    // Report the metric multiple times from this thread
                    underTest.reportStreamingMicroBatchTimeTaken(i);
                }
                try {
                    // Inject some random waits with the goal of making thread interleaving
                    // more likely so we are more likely to hit any concurrency bugs
                    int maxMillisToSleep = 200;
                    TimeUnit.MILLISECONDS.sleep(new Random().nextInt(maxMillisToSleep));
                } catch (InterruptedException e) {
                    fail();
                }
                // Each thread flushes once
                underTest.flush();
            }));
        }

        // Wait for all tasks to complete
        for (Future<?> f : futures) {
            f.get(timeoutSeconds, TimeUnit.SECONDS);
        }

        // We can't guarantee the number of put calls because sometimes the buffer will be empty
        verify(cloudwatchClient, atLeastOnce()).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());
        // But we can guarantee we sent exactly the set of metrics
        List<MetricDatum> sentMetrics = metricDatumCaptor.getAllValues().stream().flatMap(Collection::stream).toList();
        assertEquals(totalMetrics, sentMetrics.size());
    }
}
