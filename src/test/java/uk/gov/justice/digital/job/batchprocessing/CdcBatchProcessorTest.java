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
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.metrics.BufferedMetricReportingService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDC;

import java.time.Clock;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_CDC;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkDfSameTimestamp;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkSameTimestampLatest;
import static uk.gov.justice.digital.test.MinimalTestData.rowPerPkDfSameTimestamp;

@ExtendWith(MockitoExtension.class)
class CdcBatchProcessorTest extends SparkTestBase {

    private static final long batchId = 1;
    private static Dataset<Row> rowPerPk;
    private static Dataset<Row> manyRowsPerPk;

    private CdcBatchProcessor underTest;
    @Mock
    private ValidationService mockValidationService;
    @Mock
    private StructuredZoneCDC mockStructuredZone;
    @Mock
    private CuratedZoneCDC mockCuratedZone;
    @Mock
    private SourceReference mockSourceReference;
    @Mock
    private S3DataProvider mockDataProvider;
    @Mock
    private OperationalDataStoreService mockOperationalDataStoreService;
    @Mock
    private BufferedMetricReportingService mockBufferedMetricReportingService;
    @Mock
    private Dataset<Row> outputOfStructuredDf;
    @Mock
    private Dataset<Row> outputOfCuratedDf;
    @Mock
    private Clock clock;
    @Captor
    private ArgumentCaptor<Dataset<Row>> structuredArgumentCaptor;
    @Captor
    private ArgumentCaptor<Dataset<Row>> curatedArgumentCaptor;


    @BeforeAll
    static void setupClass() {
        rowPerPk = rowPerPkDfSameTimestamp(spark);
        manyRowsPerPk = manyRowsPerPkDfSameTimestamp(spark);
    }

    @BeforeEach
    void setUp() {
        underTest = new CdcBatchProcessor(
                mockValidationService,
                mockStructuredZone,
                mockCuratedZone,
                mockDataProvider,
                mockOperationalDataStoreService,
                mockBufferedMetricReportingService,
                clock
        );
    }

    @Test
    void shouldSkipEmptyBatches() {
        underTest.processBatch(mockSourceReference, spark, spark.emptyDataFrame(), batchId);

        verifyNoInteractions(mockValidationService);
        verifyNoInteractions(mockStructuredZone);
        verifyNoInteractions(mockCuratedZone);
        verifyNoInteractions(mockOperationalDataStoreService);
        verifyNoInteractions(mockBufferedMetricReportingService);
    }

    @Test
    void shouldDelegateValidation() {
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
    void shouldPassDataForStructuredZoneProcessing() {
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
    void shouldCallCuratedWithOutputOfStructured() {
        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockValidationService.handleValidation(any(), eq(rowPerPk), any(), any(), any())).thenReturn(rowPerPk);
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);
        when(mockStructuredZone.process(any(), any(), any())).thenReturn(outputOfStructuredDf);

        underTest.processBatch(mockSourceReference, spark, rowPerPk, batchId);

        verify(mockCuratedZone, times(1)).process(any(), eq(outputOfStructuredDf), eq(mockSourceReference));
    }

    @Test
    void shouldCallStructuredWithLatestRecordsByPK() {
        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockValidationService.handleValidation(any(), any(), any(), any(), any())).thenReturn(manyRowsPerPk);
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);
        when(mockStructuredZone.process(any(), any(), any())).thenReturn(manyRowsPerPk);

        underTest.processBatch(mockSourceReference, spark, manyRowsPerPk, batchId);

        verify(mockStructuredZone, times(1)).process(any(), structuredArgumentCaptor.capture(), eq(mockSourceReference));

        List<Row> expected = manyRowsPerPkSameTimestampLatest();

        List<Row> structuredActual = structuredArgumentCaptor.getValue().collectAsList();
        assertEquals(expected.size(), structuredActual.size());
        assertTrue(structuredActual.containsAll(expected));
    }

    @Test
    void shouldMergeOutputOfCuratedToOperationalDataStore() {
        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockValidationService.handleValidation(any(), eq(rowPerPk), any(), any(), any())).thenReturn(rowPerPk);
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);
        when(mockStructuredZone.process(any(), any(), any())).thenReturn(outputOfStructuredDf);
        when(mockCuratedZone.process(any(), any(), any())).thenReturn(outputOfCuratedDf);

        underTest.processBatch(mockSourceReference, spark, rowPerPk, batchId);

        verify(mockOperationalDataStoreService, times(1)).mergeData(outputOfCuratedDf, mockSourceReference);
    }

    @Test
    void shouldReportInputThroughputCount() {
        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);
        when(mockValidationService.handleValidation(any(), any(), any(), any(), any())).thenReturn(rowPerPk);
        when(mockStructuredZone.process(any(), any(), any())).thenReturn(outputOfStructuredDf);
        when(mockCuratedZone.process(any(), any(), any())).thenReturn(outputOfCuratedDf);

        underTest.processBatch(mockSourceReference, spark, manyRowsPerPk, batchId);
        verify(mockBufferedMetricReportingService, times(1)).bufferStreamingThroughputInput(manyRowsPerPk);
    }

    @Test
    void shouldReportStructuredThroughputCount() {
        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockValidationService.handleValidation(any(), any(), any(), any(), any())).thenReturn(rowPerPk);
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);
        when(mockStructuredZone.process(any(), any(), any())).thenReturn(outputOfStructuredDf);
        when(mockCuratedZone.process(any(), any(), any())).thenReturn(outputOfCuratedDf);

        underTest.processBatch(mockSourceReference, spark, manyRowsPerPk, batchId);
        verify(mockBufferedMetricReportingService, times(1)).bufferStreamingThroughputWrittenToStructured(outputOfStructuredDf);
    }

    @Test
    void shouldReportCuratedThroughputCount() {
        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockValidationService.handleValidation(any(), any(), any(), any(), any())).thenReturn(rowPerPk);
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);
        when(mockStructuredZone.process(any(), any(), any())).thenReturn(outputOfStructuredDf);
        when(mockCuratedZone.process(any(), any(), any())).thenReturn(outputOfCuratedDf);

        underTest.processBatch(mockSourceReference, spark, manyRowsPerPk, batchId);
        verify(mockBufferedMetricReportingService, times(1)).bufferStreamingThroughputWrittenToCurated(outputOfCuratedDf);
    }

    @Test
    void shouldReportMicroBatchTimeTaken() {
        when(clock.millis()).thenReturn(1000L, 2100L);

        when(mockSourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
        when(mockValidationService.handleValidation(any(), any(), any(), any(), any())).thenReturn(rowPerPk);
        when(mockDataProvider.inferSchema(any(), any(), any())).thenReturn(TEST_DATA_SCHEMA);
        when(mockStructuredZone.process(any(), any(), any())).thenReturn(outputOfStructuredDf);
        when(mockCuratedZone.process(any(), any(), any())).thenReturn(outputOfCuratedDf);

        underTest.processBatch(mockSourceReference, spark, manyRowsPerPk, batchId);
        verify(mockBufferedMetricReportingService, times(1)).bufferStreamingMicroBatchTimeTaken(1100L);
    }
}
