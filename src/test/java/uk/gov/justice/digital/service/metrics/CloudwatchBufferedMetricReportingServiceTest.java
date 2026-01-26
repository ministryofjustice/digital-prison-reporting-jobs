package uk.gov.justice.digital.service.metrics;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;
import static com.amazonaws.services.cloudwatch.model.StandardUnit.Milliseconds;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyList;
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
class CloudwatchBufferedMetricReportingServiceTest {

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
    @Captor
    private ArgumentCaptor<Collection<MetricDatum>> metricDatumCaptor;
    @Mock
    private Dataset<Row> mockDf;

    private CloudwatchBufferedMetricReportingService underTest;

    @BeforeEach
    void setUp() {
        when(jobProperties.getSparkJobName()).thenReturn(JOB);
        when(jobArguments.getCloudwatchMetricsNamespace()).thenReturn(NAMESPACE);
        when(jobArguments.getConfigKey()).thenReturn(DOMAIN);
        underTest = new CloudwatchBufferedMetricReportingService(jobArguments, jobProperties, cloudwatchClient, clock);
    }

    @Test
    void bufferViolationCountShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);

        underTest.bufferViolationCount(10L);
        underTest.flushAllBufferedMetrics();

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
    void bufferDataReconciliationResultsShouldPutMetrics() {
        DataReconciliationResults dataReconciliationResults = mock(DataReconciliationResults.class);

        when(clock.instant()).thenReturn(timestamp);
        when(dataReconciliationResults.numReconciliationChecksFailing()).thenReturn(2L);

        underTest.bufferDataReconciliationResults(dataReconciliationResults);
        underTest.flushAllBufferedMetrics();

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
    void bufferStreamingThroughputInputShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);
        when(mockDf.count()).thenReturn(100L);

        underTest.bufferStreamingThroughputInput(mockDf);
        underTest.flushAllBufferedMetrics();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingThroughputInputCount", datum.getMetricName());
        assertEquals(100L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        assertEquals(timestamp, datum.getTimestamp().toInstant());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void bufferStreamingThroughputWrittenToStructuredShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);
        when(mockDf.count()).thenReturn(100L);

        underTest.bufferStreamingThroughputWrittenToStructured(mockDf);
        underTest.flushAllBufferedMetrics();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingThroughputStructuredCount", datum.getMetricName());
        assertEquals(100L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        assertEquals(timestamp, datum.getTimestamp().toInstant());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void bufferStreamingThroughputWrittenToCuratedShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);
        when(mockDf.count()).thenReturn(100L);

        underTest.bufferStreamingThroughputWrittenToCurated(mockDf);
        underTest.flushAllBufferedMetrics();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingThroughputCuratedCount", datum.getMetricName());
        assertEquals(100L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        assertEquals(timestamp, datum.getTimestamp().toInstant());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void bufferStreamingMicroBatchTimeTakenShouldPutMetrics() {
        when(clock.instant()).thenReturn(timestamp);

        underTest.bufferStreamingMicroBatchTimeTaken(1000L);
        underTest.flushAllBufferedMetrics();

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
    void flushAllBufferedMetricsShouldPutMultipleMetricsInOneCall() {
        when(clock.instant()).thenReturn(timestamp);

        underTest.bufferStreamingThroughputWrittenToCurated(mockDf);
        underTest.bufferStreamingMicroBatchTimeTaken(1000L);
        underTest.flushAllBufferedMetrics();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();
        assertEquals(2, sentMetrics.size());
    }

    @Test
    void flushAllBufferedMetricsShouldClearMetricsBetweenCalls() {
        when(clock.instant()).thenReturn(timestamp);
        when(mockDf.count()).thenReturn(100L);

        underTest.bufferStreamingThroughputWrittenToCurated(mockDf);
        underTest.flushAllBufferedMetrics();

        underTest.bufferStreamingMicroBatchTimeTaken(1000L);
        underTest.flushAllBufferedMetrics();

        verify(cloudwatchClient, times(2)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());
    }

    @Test
    void flushAllBufferedMetricsShouldDoNothingIfThereAreNoMetricsToReport() {
        underTest.flushAllBufferedMetrics();
        verifyNoInteractions(cloudwatchClient);
    }

    @Test
    void flushAllBufferedMetricsShouldContinueWhenCloudwatchThrows() {
        when(clock.instant()).thenReturn(timestamp);
        doThrow(new AmazonClientException("Test Exception")).when(cloudwatchClient).putMetrics(anyString(), anyList());
        underTest.bufferStreamingMicroBatchTimeTaken(1000L);
        assertDoesNotThrow(() -> underTest.flushAllBufferedMetrics());
    }

    @Test
    void manyWritersSingleFlushShouldPublishCorrectly() throws ExecutionException, InterruptedException, TimeoutException {
        when(clock.instant()).thenReturn(timestamp);

        int numThreads = 10;
        int metricsPerThread = 1000;
        int totalMetrics = numThreads * metricsPerThread;

        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        CyclicBarrier startBarrier = new CyclicBarrier(numThreads);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            futures.add(pool.submit(() -> {
                try {
                    startBarrier.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    fail();
                }
                for (int i = 0; i < metricsPerThread; i++) {
                    underTest.bufferStreamingMicroBatchTimeTaken(i);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        underTest.flushAllBufferedMetrics();

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();
        assertEquals(totalMetrics, sentMetrics.size());
    }

    @Test
    void manyWritersManyFlushersShouldPublishCorrectly() throws ExecutionException, InterruptedException, TimeoutException {
        when(clock.instant()).thenReturn(timestamp);

        int numThreads = 10;
        int metricsPerThread = 1000;
        int totalMetrics = numThreads * metricsPerThread;

        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        CyclicBarrier startBarrier = new CyclicBarrier(numThreads);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            futures.add(pool.submit(() -> {
                try {
                    // Make all threads start at the same time
                    startBarrier.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    fail();
                }
                for (int i = 0; i < metricsPerThread; i++) {
                    underTest.bufferStreamingMicroBatchTimeTaken(i);
                }
                try {
                    // Try to make thread interleaving more likely so we are more likely to hit any concurrency bugs
                    TimeUnit.MILLISECONDS.sleep(new Random().nextInt(200));
                } catch (InterruptedException e) {
                    fail();
                }
                underTest.flushAllBufferedMetrics();
            }));
        }

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }

        // We can't guarantee the number of put calls because sometimes the buffer will be empty
        verify(cloudwatchClient, atLeastOnce()).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());
        // But we can guarantee we sent exactly the set of metrics
        List<MetricDatum> sentMetrics = metricDatumCaptor.getAllValues().stream().flatMap(Collection::stream).toList();
        assertEquals(totalMetrics, sentMetrics.size());
    }
}
