package uk.gov.justice.digital.zone.curated;


import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.CURATED_CDC;
import static uk.gov.justice.digital.test.Fixtures.CURATED_PATH;
import static uk.gov.justice.digital.test.Fixtures.PRIMARY_KEY_FIELD;
import static uk.gov.justice.digital.test.Fixtures.TABLE_NAME;
import static uk.gov.justice.digital.test.Fixtures.TABLE_SOURCE;
import static uk.gov.justice.digital.test.Fixtures.getAllCapturedRecords;
import static uk.gov.justice.digital.test.ZoneFixtures.*;

@ExtendWith(MockitoExtension.class)
class CuratedZoneCdcTest extends BaseSparkTest {

    @Mock
    private JobArguments mockJobArguments;

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorage;
    @Mock
    private ViolationService mockViolationService;

    @Captor
    ArgumentCaptor<Dataset<Row>> dataframeCaptor;

    private final String curatedPath = createValidatedPath(CURATED_PATH, TABLE_SOURCE, TABLE_NAME);

    private final SourceReference.PrimaryKey primaryKey = new SourceReference.PrimaryKey(PRIMARY_KEY_FIELD);

    private CuratedZone underTest;


    @BeforeEach
    public void setUp() {
        reset(mockDataStorage);
        when(mockJobArguments.getCuratedS3Path()).thenReturn(CURATED_PATH);

        underTest = new CuratedZoneCDC(
                mockJobArguments,
                mockDataStorage,
                mockViolationService
        );
    }

    @Test
    public void shouldWriteStructuredIncrementalRecordsToDeltaTable() throws DataStorageException {
        val expectedRecords = createStructuredIncrementalDataset(spark);
        val expectedRecordsInWriteOrder = createExpectedRecordsInWriteOrder(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).mergeRecords(eq(spark), eq(curatedPath), dataframeCaptor.capture(), any(), any());

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.process(spark, expectedRecords, mockSourceReference).collectAsList()
        );

        assertIterableEquals(
                expectedRecordsInWriteOrder.collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidInsertRecords() throws DataStorageException {
        val expectedRecords = createStructuredInsertDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).mergeRecords(eq(spark), eq(curatedPath), dataframeCaptor.capture(), eq(primaryKey), any());

        underTest.process(spark, expectedRecords, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidUpdateRecords() throws DataStorageException {
        val expectedRecords = createStructuredUpdateDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).mergeRecords(eq(spark), eq(curatedPath), dataframeCaptor.capture(), eq(primaryKey), any());

        underTest.process(spark, expectedRecords, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidDeleteRecords() throws DataStorageException {
        val expectedRecords = createStructuredDeleteDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).mergeRecords(eq(spark), eq(curatedPath), dataframeCaptor.capture(), eq(primaryKey), any());

        underTest.process(spark, expectedRecords, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldWriteViolationsWhenDataStorageRetriesExhausted() throws DataStorageException {
        givenTheSourceReferenceIsValid();

        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception("Some problem"));
        doThrow(thrown).when(mockDataStorage).mergeRecords(any(), any(), any(), any(), any());

        val inputDf = createStructuredUpdateDataset(spark);
        underTest.process(spark, inputDf, mockSourceReference).collect();
        verify(mockViolationService).handleRetriesExhausted(any(), eq(inputDf), eq(TABLE_SOURCE), eq(TABLE_NAME), eq(thrown), eq(CURATED_CDC));
    }

    @Test
    public void shouldReturnEmptyDataFrameWhenDataStorageRetriesExhausted() throws DataStorageException {
        givenTheSourceReferenceIsValid();

        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception("Some problem"));
        doThrow(thrown).when(mockDataStorage).mergeRecords(any(), any(), any(), any(), any());

        val inputDf = createStructuredUpdateDataset(spark);
        val resultDf = underTest.process(spark, inputDf, mockSourceReference);
        assertTrue(resultDf.isEmpty());
    }

    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getPrimaryKey()).thenReturn(primaryKey);
    }

}