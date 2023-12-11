package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDCS3;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDCS3;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_CDC;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkDfSameTimestamp;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkSameTimestampLatest;
import static uk.gov.justice.digital.test.MinimalTestData.rowPerPkDfSameTimestamp;

@ExtendWith(MockitoExtension.class)
class CdcBatchProcessorTest extends BaseSparkTest {

    private static final long batchId = 1;
    private static Dataset<Row> rowPerPk;
    private static Dataset<Row> manyRowsPerPk;

    private CdcBatchProcessor underTest;
    @Mock
    private ValidationService mockValidationService;
    @Mock
    private StructuredZoneCDCS3 mockStructuredZone;
    @Mock
    private CuratedZoneCDCS3 mockCuratedZone;
    @Mock
    private SourceReference mockSourceReference;
    @Mock
    private S3DataProvider mockDataProvider;
    @Captor
    private ArgumentCaptor<Dataset<Row>> structuredArgumentCaptor;
    @Captor
    private ArgumentCaptor<Dataset<Row>> curatedArgumentCaptor;


    @BeforeAll
    public static void setupClass() {
        rowPerPk = rowPerPkDfSameTimestamp(spark);
        manyRowsPerPk = manyRowsPerPkDfSameTimestamp(spark);
    }

    @BeforeEach
    public void setUp() {
        underTest = new CdcBatchProcessor(mockValidationService, mockStructuredZone, mockCuratedZone, mockDataProvider);
    }

    @Test
    public void shouldSkipEmptyBatches() {
        underTest.processBatch(mockSourceReference, spark, spark.emptyDataFrame(), batchId);

        verify(mockValidationService, times(0)).handleValidation(any(), any(), any(), any(), any());
        verify(mockStructuredZone, times(0)).process(any(), any(), any());
        verify(mockCuratedZone, times(0)).process(any(), any(), any());
    }

    @Test
    public void shouldDelegateValidation() {
        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockValidationService.handleValidation(any(), eq(rowPerPk), any(), any(), any())).thenReturn(rowPerPk);
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);

        underTest.processBatch(mockSourceReference, spark, rowPerPk, batchId);

        verify(mockValidationService, times(1))
                .handleValidation(spark, rowPerPk, mockSourceReference, TEST_DATA_SCHEMA, STRUCTURED_CDC);
    }

    @Test
    public void shouldPassDataForStructuredZoneProcessing() {
        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockValidationService.handleValidation(any(), eq(rowPerPk), any(), any(), any())).thenReturn(rowPerPk);
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);
        underTest.processBatch(mockSourceReference, spark, rowPerPk, batchId);

        verify(mockStructuredZone, times(1)).process(any(), structuredArgumentCaptor.capture(), eq(mockSourceReference));

        List<Row> expected = rowPerPk.collectAsList();
        List<Row> result = structuredArgumentCaptor.getValue().collectAsList();
        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    public void shouldPassDataForCuratedZoneProcessing() {
        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockValidationService.handleValidation(any(), eq(rowPerPk), any(), any(), any())).thenReturn(rowPerPk);
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);

        underTest.processBatch(mockSourceReference, spark, rowPerPk, batchId);

        verify(mockCuratedZone, times(1)).process(any(), curatedArgumentCaptor.capture(), eq(mockSourceReference));

        List<Row> expected = rowPerPk.collectAsList();
        List<Row> result = curatedArgumentCaptor.getValue().collectAsList();
        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    public void shouldWriteLatestRecordsByPK() {
        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockValidationService.handleValidation(any(), any(), any(), any(), any())).thenReturn(manyRowsPerPk);
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);

        underTest.processBatch(mockSourceReference, spark, manyRowsPerPk, batchId);

        verify(mockStructuredZone, times(1)).process(any(), structuredArgumentCaptor.capture(), eq(mockSourceReference));
        verify(mockCuratedZone, times(1)).process(any(), curatedArgumentCaptor.capture(), eq(mockSourceReference));

        List<Row> expected = manyRowsPerPkSameTimestampLatest();

        List<Row> structuredActual = structuredArgumentCaptor.getValue().collectAsList();
        assertEquals(expected.size(), structuredActual.size());
        assertTrue(structuredActual.containsAll(expected));

        List<Row> curatedActual = curatedArgumentCaptor.getValue().collectAsList();
        assertEquals(expected.size(), curatedActual.size());
        assertTrue(curatedActual.containsAll(expected));
    }
}