package uk.gov.justice.digital.job.cdc;

import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecutionException;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.service.metrics.BatchMetrics;
import uk.gov.justice.digital.test.TestBatchedMetricReportingService;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TableStreamingQueryProviderTest {

    private static final String sourceName = "source";
    private static final String tableName = "table";
    @Mock
    private JobArguments arguments;
    @Mock
    private S3DataProvider dataProvider;
    @Mock
    private CdcBatchProcessor batchProcessor;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private ViolationService violationService;
    @Mock
    private SparkSession spark;
    @Mock
    private Dataset<Row> df;
    @Mock
    private BatchMetrics batchMetrics;
    private TableStreamingQueryProvider underTest;

    @BeforeEach
    void setUp() {
        TestBatchedMetricReportingService stubbedMetricReportingService = new TestBatchedMetricReportingService(batchMetrics);
        underTest = spy(new TableStreamingQueryProvider(
                arguments,
                dataProvider,
                batchProcessor,
                sourceReferenceService,
                violationService,
                stubbedMetricReportingService
        ));
    }

    @Test
    void shouldCreateStandardProcessingQueryUnderNormalCircumstances() {
        when(sourceReferenceService.getSourceReference(sourceName, tableName)).thenReturn(Optional.of(sourceReference));
        when(dataProvider.getStreamingSourceData(spark, sourceReference)).thenReturn(df);
        underTest.provide(spark, sourceName, tableName);

        verify(underTest, times(1)).standardProcessingQuery(any(), eq(sourceName), eq(tableName), eq(sourceReference));
    }

    @Test
    void shouldCreateNoSchemaFoundQueryWhenNoSourceReference() {
        when(sourceReferenceService.getSourceReference(sourceName, tableName)).thenReturn(Optional.empty());
        underTest.provide(spark, sourceName, tableName);

        verify(underTest, times(1)).noSchemaFoundQuery(any(), eq(sourceName), eq(tableName));
    }

    @Test
    void shouldDecorateBatchFuncWithIncompatibleSchemaHandling() {
        when(sourceReferenceService.getSourceReference(sourceName, tableName)).thenReturn(Optional.empty());
        underTest.provide(spark, sourceName, tableName);

        verify(underTest, times(1)).withIncompatibleSchemaHandling(eq(sourceName), eq(tableName), any(), any());
    }

    @Test
    void incompatibleSchemaHandlingShouldWriteCdcDataToViolationsWhenSchemaIsIncompatible() throws Exception {
        when(df.sparkSession()).thenReturn(spark);
        SchemaColumnConvertNotSupportedException ultimateCause =
                new SchemaColumnConvertNotSupportedException("col", "physical type", "logical type");
        SparkException toThrow = new SparkException("", new QueryExecutionException("", ultimateCause));

        VoidFunction2<Dataset<Row>, Long> decoratedFunc = underTest.withIncompatibleSchemaHandling(sourceName, tableName, batchMetrics, (dataFrame, batchId) -> {
            throw toThrow;
        });

        long arbitraryBatchId = 1L;
        decoratedFunc.call(df, arbitraryBatchId);

        verify(violationService, times(1))
                .writeCdcDataToViolations(any(), any(), eq(sourceName), eq(tableName), anyString());

    }
}
