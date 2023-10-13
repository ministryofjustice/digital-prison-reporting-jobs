package uk.gov.justice.digital.zone.structured;


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
import static uk.gov.justice.digital.test.ZoneFixtures.createStructuredDeleteDataset;
import static uk.gov.justice.digital.test.ZoneFixtures.createStructuredIncrementalDataset;
import static uk.gov.justice.digital.test.ZoneFixtures.createStructuredInsertDataset;
import static uk.gov.justice.digital.test.ZoneFixtures.createStructuredUpdateDataset;
import static uk.gov.justice.digital.test.ZoneFixtures.createTestDataset;

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

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).upsertRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey));
        doNothing().when(mockDataStorage).updateRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey));
        doNothing().when(mockDataStorage).deleteRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey));

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.process(spark, testDataSet, mockSourceReference).collectAsList()
        );

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidInsertRecords() throws DataStorageException {
        val expectedRecords = createStructuredInsertDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).upsertRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey));

        underTest.process(spark, testDataSet, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidUpdateRecords() throws DataStorageException {
        val expectedRecords = createStructuredUpdateDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).updateRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey));

        underTest.process(spark, testDataSet, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidDeleteRecords() throws DataStorageException {
        val expectedRecords = createStructuredDeleteDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).deleteRecords(eq(spark), eq(structuredPath), dataframeCaptor.capture(), eq(primaryKey));

        underTest.process(spark, testDataSet, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
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
        doNothing().when(mockDataStorage).upsertRecords(any(), any(), any(), any());

        val structuredIncrementalRecords = underTest.process(spark, testDataSet, mockSourceReference);

        assertTrue(hasNullColumns(structuredIncrementalRecords));
    }
    @Test
    public void shouldWriteViolationsWhenDataStorageRetriesExhausted() throws DataStorageException {
        givenTheSourceReferenceIsValid();

        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception("Some problem"));
        doThrow(thrown).when(mockDataStorage).upsertRecords(any(), any(), any(), any());

        underTest.process(spark, testDataSet, mockSourceReference).collect();
        verify(mockViolationService).handleRetriesExhausted(any(), any(), eq(TABLE_SOURCE), eq(TABLE_NAME), eq(thrown), eq(STRUCTURED_CDC));
    }

    @Test
    public void shouldReturnEmptyDataFrameWhenDataStorageRetriesExhausted() throws DataStorageException {
        givenTheSourceReferenceIsValid();

        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception("Some problem"));
        doThrow(thrown).when(mockDataStorage).upsertRecords(any(), any(), any(), any());

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