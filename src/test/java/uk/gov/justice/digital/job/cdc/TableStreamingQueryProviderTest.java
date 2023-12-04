package uk.gov.justice.digital.job.cdc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.SchemaMismatchException;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
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
    private TableStreamingQueryProvider underTest;

    @BeforeEach
    public void setUp() {
        underTest = spy(new TableStreamingQueryProvider(
                arguments,
                dataProvider,
                batchProcessor,
                sourceReferenceService,
                violationService
        ));
    }

    @Test
    public void shouldCreateStandardProcessingQueryUnderNormalCircumstances() throws Exception {
        when(sourceReferenceService.getSourceReference(sourceName, tableName)).thenReturn(Optional.of(sourceReference));
        when(dataProvider.getStreamingSourceData(spark, sourceReference)).thenReturn(df);
        underTest.create(spark, sourceName, tableName);

        verify(underTest, times(1)).standardProcessingQuery(any(), eq(sourceName), eq(tableName), eq(sourceReference));
    }

    @Test
    public void shouldCreateNoSchemaFoundQueryWhenNoSourceReference() {
        when(sourceReferenceService.getSourceReference(sourceName, tableName)).thenReturn(Optional.empty());
        underTest.create(spark, sourceName, tableName);

        verify(underTest, times(1)).noSchemaFoundQuery(any(), eq(sourceName), eq(tableName));
    }

    @Test
    public void shouldCreateSchemaMismatchQueryWhenThereIsASchemaMismatch() throws Exception {
        when(sourceReferenceService.getSourceReference(sourceName, tableName)).thenReturn(Optional.of(sourceReference));
        when(dataProvider.getStreamingSourceData(spark, sourceReference)).thenThrow(new SchemaMismatchException(""));
        underTest.create(spark, sourceName, tableName);

        verify(underTest, times(1)).schemaMismatchQuery(any(), eq(sourceName), eq(tableName), eq(sourceReference));
    }

}