package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkDfSameTimestamp;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkSameTimestampLatest;
import static uk.gov.justice.digital.test.MinimalTestData.primaryKey;
import static uk.gov.justice.digital.test.MinimalTestData.rowPerPkDfSameTimestamp;

@ExtendWith(MockitoExtension.class)
class CdcBatchProcessorTest extends BaseSparkTest {

    private static final long batchId = 1;
    private static final String structuredTablePath = "/structured/path";
    private static final String curatedTablePath = "/curated/path";
    private static Dataset<Row> rowPerPk;
    private static Dataset<Row> manyRowsPerPk;

    private CdcBatchProcessor underTest;

    @Mock
    private ViolationService mockViolationService;
    @Mock
    private ValidationService mockValidationService;
    @Mock
    private DataStorageService mockDataStorageService;
    @Mock
    private SourceReference mockSourceReference;


    @BeforeAll
    public static void setupClass() {
        rowPerPk = rowPerPkDfSameTimestamp(spark);
        manyRowsPerPk = manyRowsPerPkDfSameTimestamp(spark);
    }

    @BeforeEach
    public void setUp() {
        underTest = new CdcBatchProcessor(mockViolationService, mockValidationService, mockDataStorageService);

        when(mockSourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(mockSourceReference.getSource()).thenReturn("source");
        when(mockSourceReference.getTable()).thenReturn("table");
    }

    @Test
    public void shouldDelegateValidation() {
        when(mockValidationService.handleValidation(any(), eq(rowPerPk), any())).thenReturn(rowPerPk);

        underTest.processBatch(mockSourceReference, spark, rowPerPk, batchId, structuredTablePath, curatedTablePath);

        verify(mockValidationService, times(1)).handleValidation(spark, rowPerPk, mockSourceReference);
    }

    @Test
    public void shouldWriteDataToStructured() throws DataStorageRetriesExhaustedException {
        when(mockValidationService.handleValidation(any(), eq(rowPerPk), any())).thenReturn(rowPerPk);
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        underTest.processBatch(mockSourceReference, spark, rowPerPk, batchId, structuredTablePath, curatedTablePath);

        verify(mockDataStorageService, times(1)).mergeRecordsCdc(any(), eq(structuredTablePath), argumentCaptor.capture(), eq(primaryKey));

        List<Row> expected = rowPerPk.collectAsList();
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    public void shouldWriteDataToCurated() throws DataStorageRetriesExhaustedException {
        when(mockValidationService.handleValidation(any(), eq(rowPerPk), any())).thenReturn(rowPerPk);
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        underTest.processBatch(mockSourceReference, spark, rowPerPk, batchId, structuredTablePath, curatedTablePath);

        verify(mockDataStorageService, times(1)).mergeRecordsCdc(any(), eq(curatedTablePath), argumentCaptor.capture(), eq(primaryKey));
        verify(mockDataStorageService, times(1)).updateDeltaManifestForTable(any(), eq(curatedTablePath));

        List<Row> expected = rowPerPk.collectAsList();
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    public void shouldWriteLatestRecordsByPK() throws DataStorageRetriesExhaustedException {
        when(mockValidationService.handleValidation(any(), any(), any())).thenReturn(manyRowsPerPk);

        ArgumentCaptor<Dataset<Row>> structuredArgumentCaptor = ArgumentCaptor.forClass(Dataset.class);
        ArgumentCaptor<Dataset<Row>> curatedArgumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        underTest.processBatch(mockSourceReference, spark, manyRowsPerPk, batchId, structuredTablePath, curatedTablePath);

        verify(mockDataStorageService, times(1)).mergeRecordsCdc(any(), eq(structuredTablePath), structuredArgumentCaptor.capture(), eq(primaryKey));
        verify(mockDataStorageService, times(1)).mergeRecordsCdc(any(), eq(curatedTablePath), curatedArgumentCaptor.capture(), eq(primaryKey));
        verify(mockDataStorageService, times(1)).updateDeltaManifestForTable(any(), eq(curatedTablePath));

        List<Row> expected = manyRowsPerPkSameTimestampLatest();

        List<Row> structuredActual = structuredArgumentCaptor.getValue().collectAsList();
        assertEquals(expected.size(), structuredActual.size());
        assertTrue(structuredActual.containsAll(expected));

        List<Row> curatedActual = curatedArgumentCaptor.getValue().collectAsList();
        assertEquals(expected.size(), curatedActual.size());
        assertTrue(curatedActual.containsAll(expected));
    }

    @Test
    public void shouldHandleRetriesExhausted() throws DataStorageRetriesExhaustedException {
        when(mockValidationService.handleValidation(any(), eq(rowPerPk), any())).thenReturn(rowPerPk);
        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception());
        doThrow(thrown).when(mockDataStorageService).mergeRecordsCdc(any(), any(), any(), any());
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        underTest.processBatch(mockSourceReference, spark, rowPerPk, batchId, structuredTablePath, curatedTablePath);

        verify(mockViolationService, times(1)).handleRetriesExhausted(
                any(), argumentCaptor.capture(), eq("source"), eq("table"), eq(thrown), eq(ViolationService.ZoneName.CDC)
        );

        List<Row> expected = rowPerPk.collectAsList();
        List<Row> curatedActual = argumentCaptor.getValue().collectAsList();
        assertEquals(expected.size(), curatedActual.size());
        assertTrue(curatedActual.containsAll(expected));
    }

}