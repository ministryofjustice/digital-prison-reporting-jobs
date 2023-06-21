package uk.gov.justice.digital.zone;


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
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.KEY;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.zone.Fixtures.*;
import static uk.gov.justice.digital.zone.RawZone.PRIMARY_KEY_NAME;
import static uk.gov.justice.digital.zone.StructuredZoneFixtures.*;

@ExtendWith(MockitoExtension.class)
class StructuredZoneTest extends BaseSparkTest {

    @Mock
    private JobArguments mockJobArguments;

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorage;

    @Mock
    private SourceReferenceService mockSourceReferenceService;

    @Captor
    ArgumentCaptor<Dataset<Row>> dataframeCaptor;

    private StructuredZone underTest;

    private final Dataset<Row> testDataSet = createTestDataset();


    @BeforeEach
    public void setUp() {
        reset(mockDataStorage);
        when(mockJobArguments.getViolationsS3Path()).thenReturn(VIOLATIONS_PATH);
        when(mockJobArguments.getStructuredS3Path()).thenReturn(STRUCTURED_PATH);

        underTest = new StructuredZone(
                mockJobArguments,
                mockDataStorage,
                mockSourceReferenceService
        );
    }

    @Test
    public void shouldHandleValidLoadRecords() throws DataStorageException {
        val expectedRecords = createExpectedLoadDataset();
        val structuredPath = createValidatedPath(STRUCTURED_PATH, TABLE_SOURCE, TABLE_NAME);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doNothing()
                .when(mockDataStorage)
                .appendDistinct(eq(structuredPath), dataframeCaptor.capture(), any());

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.process(spark, testDataSet, dataMigrationEventRow, false).collectAsList()
        );

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                dataframeCaptor.getValue().collectAsList()
            );
    }

    @Test
    public void shouldHandleValidIncrementalRecords() throws DataStorageException {
        val expectedRecords = createExpectedIncrementalDataset();
        val structuredPath = createValidatedPath(STRUCTURED_PATH, TABLE_SOURCE, TABLE_NAME);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).appendDistinct(eq(structuredPath), dataframeCaptor.capture(), any());
        doNothing().when(mockDataStorage).updateRecords(eq(structuredPath), dataframeCaptor.capture(), any());
        doNothing().when(mockDataStorage).deleteRecords(eq(structuredPath), dataframeCaptor.capture(), eq(PRIMARY_KEY_NAME));

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.process(spark, testDataSet, dataMigrationEventRow, true).collectAsList()
        );

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords()
        );
    }

    @Test
    public void shouldHandleValidInsertRecords() throws DataStorageException {
        val expectedRecords = createExpectedInsertDataset();
        val structuredPath = createValidatedPath(STRUCTURED_PATH, TABLE_SOURCE, TABLE_NAME);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).appendDistinct(eq(structuredPath), dataframeCaptor.capture(), any());

        underTest.process(spark, testDataSet, dataMigrationEventRow, true).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords()
        );
    }

    @Test
    public void shouldHandleValidUpdateRecords() throws DataStorageException {
        val expectedRecords = createExpectedUpdateDataset();
        val structuredPath = createValidatedPath(STRUCTURED_PATH, TABLE_SOURCE, TABLE_NAME);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).updateRecords(eq(structuredPath), dataframeCaptor.capture(), any());

        underTest.process(spark, testDataSet, dataMigrationEventRow, true).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords()
        );
    }

    @Test
    public void shouldHandleValidDeleteRecords() throws DataStorageException {
        val expectedRecords = createExpectedDeleteDataset();
        val structuredPath = createValidatedPath(STRUCTURED_PATH, TABLE_SOURCE, TABLE_NAME);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).deleteRecords(eq(structuredPath), dataframeCaptor.capture(), eq(PRIMARY_KEY_NAME));

        underTest.process(spark, testDataSet, dataMigrationEventRow, true).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords()
        );
    }

    @Test
    public void shouldContinueWhenInsertFails() throws DataStorageException {
        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doThrow(new DataStorageException("insert failed")).when(mockDataStorage).appendDistinct(any(), any(), any());

        underTest.process(spark, testDataSet, dataMigrationEventRow, true).collect();

        assertTrue(getAllCapturedRecords().isEmpty());
    }

    @Test
    public void shouldContinueWhenUpdateFails() throws DataStorageException {
        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doThrow(new DataStorageException("update failed")).when(mockDataStorage).updateRecords(any(), any(), any());

        underTest.process(spark, testDataSet, dataMigrationEventRow, true).collect();

        assertTrue(getAllCapturedRecords().isEmpty());
    }

    @Test
    public void shouldContinueWhenDeletionFails() throws DataStorageException {
        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doThrow(new DataStorageException("deletion failed")).when(mockDataStorage).deleteRecords(any(), any(), any());

        underTest.process(spark, testDataSet, dataMigrationEventRow, true).collect();

        assertTrue(getAllCapturedRecords().isEmpty());
    }

    @Test
    public void shouldHandleInvalidRecords() throws DataStorageException {
        for (Boolean isCDC: Arrays.asList(true, false)) {
            givenTheSchemaExists();
            givenTheSourceReferenceIsValid();

            assertNotNull(underTest.process(spark, testDataSet, dataMigrationEventRow, isCDC));
        }
    }

    @Test
    public void shouldHandleNoSchemaFound() throws DataStorageException {
        for (Boolean isCDC: Arrays.asList(true, false)) {
            givenTheSchemaDoesNotExist();

            assertTrue(underTest.process(spark, testDataSet, dataMigrationEventRow, isCDC).isEmpty());
        }
    }

    private void givenTheSchemaExists() {
        when(mockSourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME))
                .thenReturn(Optional.of(mockSourceReference));
    }

    private void givenTheSchemaDoesNotExist() {
        when(mockSourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME)).thenReturn(Optional.empty());
    }

    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getSchema()).thenReturn(JSON_DATA_SCHEMA);
        when(mockSourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey(KEY));
    }

    private Dataset<Row> createTestDataset() {
        val rawData = new ArrayList<>(Arrays.asList(
                record1Load,
                record2Load,
                record3Load,
                record4Insert,
                record5Insert,
                record6Insert,
                record7Update,
                record6Deletion,
                record5Update
        ));

        return spark.createDataFrame(rawData, ROW_SCHEMA);
    }

    private Dataset<Row> createExpectedLoadDataset() {
        val expectedLoadData = new ArrayList<>(Arrays.asList(
                structuredRecord2Load,
                structuredRecord3Load,
                structuredRecord1Load
        ));

        return spark.createDataFrame(expectedLoadData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    private Dataset<Row> createExpectedIncrementalDataset() {
        val expectedIncrementalData = new ArrayList<>(Arrays.asList(
                structuredRecord4Insertion,
                structuredRecord5Insertion,
                structuredRecord6Insertion,
                structuredRecord7Update,
                structuredRecord6Deletion,
                structuredRecord5Update
        ));

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    private Dataset<Row> createExpectedInsertDataset() {
        val expectedIncrementalData = new ArrayList<>(Arrays.asList(
                structuredRecord4Insertion,
                structuredRecord5Insertion,
                structuredRecord6Insertion
        ));

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    private Dataset<Row> createExpectedUpdateDataset() {
        val expectedIncrementalData = new ArrayList<>(Arrays.asList(
                structuredRecord7Update,
                structuredRecord5Update
        ));

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    private Dataset<Row> createExpectedDeleteDataset() {
        val expectedIncrementalData = new ArrayList<>(Collections.singletonList(structuredRecord6Deletion));

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    private List<Row> getAllCapturedRecords() {
        return dataframeCaptor
                .getAllValues()
                .stream()
                .flatMap(x -> x.collectAsList().stream())
                .collect(Collectors.toList());
    }

}