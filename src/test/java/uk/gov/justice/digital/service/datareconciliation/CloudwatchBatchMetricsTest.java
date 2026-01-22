package uk.gov.justice.digital.service.datareconciliation;

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
import uk.gov.justice.digital.service.metrics.CloudwatchBatchMetrics;

import java.util.Collection;
import java.util.List;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;
import static com.amazonaws.services.cloudwatch.model.StandardUnit.Milliseconds;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CloudwatchBatchMetricsTest {

    private static final String DOMAIN = "some domain";
    private static final String JOB = "some job";
    private static final String NAMESPACE = "SomeNamespace";
    @Mock
    private JobArguments jobArguments;
    @Mock
    private JobProperties jobProperties;
    @Mock
    private CloudwatchClient cloudwatchClient;
    @Captor
    private ArgumentCaptor<Collection<MetricDatum>> metricDatumCaptor;
    @Mock
    private Dataset<Row> mockDf;

    private CloudwatchBatchMetrics underTest;

    @BeforeEach
    void setUp() {
        underTest = new CloudwatchBatchMetrics(jobArguments, jobProperties, cloudwatchClient);
    }

    @Test
    void bufferViolationCountShouldPutMetrics() {

        when(jobProperties.getSparkJobName()).thenReturn(JOB);
        when(jobArguments.getCloudwatchMetricsNamespace()).thenReturn(NAMESPACE);

        underTest.bufferViolationCount(10L);

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobViolationCount", datum.getMetricName());
        assertEquals(10L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void bufferDataReconciliationResultsShouldPutMetrics() {

        DataReconciliationResults dataReconciliationResults = mock(DataReconciliationResults.class);

        when(jobArguments.getConfigKey()).thenReturn(DOMAIN);
        when(jobArguments.getCloudwatchMetricsNamespace()).thenReturn(NAMESPACE);
        when(dataReconciliationResults.numReconciliationChecksFailing()).thenReturn(2L);

        underTest.bufferDataReconciliationResults(dataReconciliationResults);

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("FailedReconciliationChecks", datum.getMetricName());
        assertEquals(2L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("InputDomain", dimension.getName());
        assertEquals(DOMAIN, dimension.getValue());
    }

    @Test
    void bufferStreamingThroughputInputShouldPutMetrics() {

        when(jobProperties.getSparkJobName()).thenReturn(JOB);
        when(jobArguments.getCloudwatchMetricsNamespace()).thenReturn(NAMESPACE);
        when(mockDf.count()).thenReturn(100L);

        underTest.bufferStreamingThroughputInput(mockDf);

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingThroughputInputCount", datum.getMetricName());
        assertEquals(100L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void bufferStreamingThroughputWrittenToStructuredShouldPutMetrics() {

        when(jobProperties.getSparkJobName()).thenReturn(JOB);
        when(jobArguments.getCloudwatchMetricsNamespace()).thenReturn(NAMESPACE);
        when(mockDf.count()).thenReturn(100L);

        underTest.bufferStreamingThroughputWrittenToStructured(mockDf);

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingThroughputStructuredCount", datum.getMetricName());
        assertEquals(100L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void bufferStreamingThroughputWrittenToCuratedShouldPutMetrics() {

        when(jobProperties.getSparkJobName()).thenReturn(JOB);
        when(jobArguments.getCloudwatchMetricsNamespace()).thenReturn(NAMESPACE);
        when(mockDf.count()).thenReturn(100L);

        underTest.bufferStreamingThroughputWrittenToCurated(mockDf);

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingThroughputCuratedCount", datum.getMetricName());
        assertEquals(100L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }

    @Test
    void bufferStreamingMicroBatchTimeTakenShouldPutMetrics() {

        when(jobProperties.getSparkJobName()).thenReturn(JOB);
        when(jobArguments.getCloudwatchMetricsNamespace()).thenReturn(NAMESPACE);

        underTest.bufferStreamingMicroBatchTimeTaken(1000L);

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("GlueJobStreamingMicroBatchTime", datum.getMetricName());
        assertEquals(1000L, datum.getValue());
        assertEquals(Milliseconds.toString(), datum.getUnit());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("JobName", dimension.getName());
        assertEquals(JOB, dimension.getValue());
    }
}
