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

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.KEY;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.zone.fixtures.Fixtures.*;
import static uk.gov.justice.digital.zone.RawZone.PRIMARY_KEY_NAME;
import static uk.gov.justice.digital.zone.fixtures.ZoneFixtures.*;

@ExtendWith(MockitoExtension.class)
class CuratedZoneCdcTest extends BaseSparkTest {

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

    String curatedPath = createValidatedPath(CURATED_PATH, TABLE_SOURCE, TABLE_NAME);

    private CuratedZone underTest;


    @BeforeEach
    public void setUp() {
        reset(mockDataStorage);
        when(mockJobArguments.getCuratedS3Path()).thenReturn(CURATED_PATH);

        underTest = new CuratedZoneCDC(
                mockJobArguments,
                mockDataStorage,
                mockSourceReferenceService
        );
    }

    @Test
    public void shouldWriteStructuredIncrementalRecordsToDeltaTable() throws DataStorageException {
        val expectedRecords = createStructuredIncrementalDataset(spark);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).appendDistinct(eq(curatedPath), dataframeCaptor.capture(), any());
        doNothing().when(mockDataStorage).updateRecords(eq(curatedPath), dataframeCaptor.capture(), any());
        doNothing().when(mockDataStorage).deleteRecords(eq(curatedPath), dataframeCaptor.capture(), eq(PRIMARY_KEY_NAME));

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.process(spark, expectedRecords, dataMigrationEventRow).collectAsList()
        );

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidInsertRecords() throws DataStorageException {
        val expectedRecords = createStructuredInsertDataset(spark);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).appendDistinct(eq(curatedPath), dataframeCaptor.capture(), any());

        underTest.process(spark, expectedRecords, dataMigrationEventRow).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidUpdateRecords() throws DataStorageException {
        val expectedRecords = createStructuredUpdateDataset(spark);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).updateRecords(eq(curatedPath), dataframeCaptor.capture(), any());

        underTest.process(spark, expectedRecords, dataMigrationEventRow).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidDeleteRecords() throws DataStorageException {
        val expectedRecords = createStructuredDeleteDataset(spark);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).deleteRecords(eq(curatedPath), dataframeCaptor.capture(), eq(PRIMARY_KEY_NAME));

        underTest.process(spark, expectedRecords, dataMigrationEventRow).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldContinueWhenInsertFails() throws DataStorageException {
        val records = createStructuredInsertDataset(spark);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doThrow(new DataStorageException("insert failed")).when(mockDataStorage).appendDistinct(any(), any(), any());

        underTest.process(spark, records, dataMigrationEventRow).collect();

        assertTrue(getAllCapturedRecords(dataframeCaptor).isEmpty());
    }

    @Test
    public void shouldContinueWhenUpdateFails() throws DataStorageException {
        val records = createStructuredUpdateDataset(spark);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doThrow(new DataStorageException("update failed")).when(mockDataStorage).updateRecords(any(), any(), any());

        underTest.process(spark, records, dataMigrationEventRow).collect();

        assertTrue(getAllCapturedRecords(dataframeCaptor).isEmpty());
    }

    @Test
    public void shouldContinueWhenDeletionFails() throws DataStorageException {
        val records = createStructuredDeleteDataset(spark);

        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doThrow(new DataStorageException("deletion failed")).when(mockDataStorage).deleteRecords(any(), any(), any());

        underTest.process(spark, records, dataMigrationEventRow).collect();

        assertTrue(getAllCapturedRecords(dataframeCaptor).isEmpty());
    }

    private void givenTheSchemaExists() {
        when(mockSourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME))
                .thenReturn(Optional.of(mockSourceReference));
    }

    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey(KEY));
    }

}