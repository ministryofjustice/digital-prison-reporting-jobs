package uk.gov.justice.digital.zone.structured;


import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_CDC;
import static uk.gov.justice.digital.test.Fixtures.JSON_DATA_SCHEMA;
import static uk.gov.justice.digital.test.Fixtures.PRIMARY_KEY_FIELD;
import static uk.gov.justice.digital.test.Fixtures.STRUCTURED_PATH;
import static uk.gov.justice.digital.test.Fixtures.TABLE_NAME;
import static uk.gov.justice.digital.test.Fixtures.TABLE_SOURCE;
import static uk.gov.justice.digital.test.Fixtures.VIOLATIONS_PATH;
import static uk.gov.justice.digital.test.Fixtures.getAllCapturedRecords;
import static uk.gov.justice.digital.test.Fixtures.hasNullColumns;
import static uk.gov.justice.digital.test.ZoneFixtures.*;

@ExtendWith(MockitoExtension.class)
class StructuredZoneCdcTest extends BaseSparkTest {

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

    private StructuredZone underTest;

    private final Dataset<Row> testDataSet = createTestDataset(spark);
    private final Dataset<Row> testNonUniqueUnorderedDataSet = createNonUniqueUnorderedTestDataset(spark);

    private final String structuredPath = createValidatedPath(STRUCTURED_PATH, TABLE_SOURCE, TABLE_NAME);

    private final SourceReference.PrimaryKey primaryKey = new SourceReference.PrimaryKey(PRIMARY_KEY_FIELD);

    @BeforeEach
    public void setUp() {
        reset(mockDataStorage);
        when(mockJobArguments.getViolationsS3Path()).thenReturn(VIOLATIONS_PATH);
        when(mockJobArguments.getStructuredS3Path()).thenReturn(STRUCTURED_PATH);

        underTest = new StructuredZoneCDC(
                mockJobArguments,
                mockDataStorage,
                mockViolationService
        );
    }

    @Test
    public void shouldHandleValidIncrementalRecords() throws DataStorageException {
        val expectedRecords = createStructuredIncrementalDataset(spark);
        val expectedRecordsInWriteOrder = createExpectedRecordsInWriteOrder(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).mergeRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey), any());

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.process(spark, testDataSet, mockSourceReference).collectAsList()
        );

        assertIterableEquals(
                expectedRecordsInWriteOrder.collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleNonUniqueOutOfOrderIncrementalRecords() throws DataStorageException {
        val expectedUniqueMostRecentRecordsInWriteOrder = createExpectedUniqueMostRecentRecordsInWriteOrder(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).mergeRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey), any());

        assertIterableEquals(
                expectedUniqueMostRecentRecordsInWriteOrder.collectAsList(),
                underTest.process(spark, testNonUniqueUnorderedDataSet, mockSourceReference).collectAsList()
        );

        assertIterableEquals(
                expectedUniqueMostRecentRecordsInWriteOrder.collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidInsertRecords() throws DataStorageException {
        val expectedRecords = createStructuredInsertDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).mergeRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey), any());

        val insertRecords = testDataSet.filter(functions.col(OPERATION).equalTo(Insert.getName()));
        underTest.process(spark, insertRecords, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidUpdateRecords() throws DataStorageException {
        val expectedRecords = createStructuredUpdateDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).mergeRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey), any());

        val updateRecords = testDataSet.filter(functions.col(OPERATION).equalTo(Update.getName()));
        underTest.process(spark, updateRecords, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidDeleteRecords() throws DataStorageException {
        val expectedRecords = createStructuredDeleteDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).mergeRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey), any());

        val deleteRecords = testDataSet.filter(functions.col(OPERATION).equalTo(Delete.getName()));
        underTest.process(spark, deleteRecords, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleInvalidRecords() throws DataStorageException {
        givenTheSourceReferenceIsValid();

        assertNotNull(underTest.process(spark, testDataSet, mockSourceReference));
    }

    @Test
    public void shouldKeepNullColumnsInData() throws DataStorageException {
        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).mergeRecords(any(), any(), any(), any(), any());

        val structuredIncrementalRecords = underTest.process(spark, testDataSet, mockSourceReference);

        assertTrue(hasNullColumns(structuredIncrementalRecords));
    }
    @Test
    public void shouldWriteViolationsWhenDataStorageRetriesExhausted() throws DataStorageException {
        givenTheSourceReferenceIsValid();

        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception("Some problem"));
        doThrow(thrown).when(mockDataStorage).mergeRecords(any(), any(), any(), any(), any());

        underTest.process(spark, testDataSet, mockSourceReference).collect();
        verify(mockViolationService).handleRetriesExhausted(any(), any(), eq(TABLE_SOURCE), eq(TABLE_NAME), eq(thrown), eq(STRUCTURED_CDC));
    }

    @Test
    public void shouldReturnEmptyDataFrameWhenDataStorageRetriesExhausted() throws DataStorageException {
        givenTheSourceReferenceIsValid();

        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception("Some problem"));
        doThrow(thrown).when(mockDataStorage).mergeRecords(any(), any(), any(), any(), any());

        val resultDf = underTest.process(spark, testDataSet, mockSourceReference);
        assertTrue(resultDf.isEmpty());
    }


    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getSchema()).thenReturn(JSON_DATA_SCHEMA);
        when(mockSourceReference.getPrimaryKey()).thenReturn(primaryKey);
    }

}