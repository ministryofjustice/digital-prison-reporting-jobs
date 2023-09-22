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
import uk.gov.justice.digital.service.DataStorageService;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.test.Fixtures.*;
import static uk.gov.justice.digital.test.ZoneFixtures.*;

@ExtendWith(MockitoExtension.class)
class CuratedZoneCdcTest extends BaseSparkTest {

    @Mock
    private JobArguments mockJobArguments;

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorage;

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
                mockDataStorage
        );
    }

    @Test
    public void shouldWriteStructuredIncrementalRecordsToDeltaTable() throws DataStorageException {
        val expectedRecords = createStructuredIncrementalDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).upsertRecords(eq(spark), eq(curatedPath), dataframeCaptor.capture(), any());
        doNothing().when(mockDataStorage).updateRecords(eq(spark), eq(curatedPath), dataframeCaptor.capture(), any());
        doNothing().when(mockDataStorage).deleteRecords(eq(spark), eq(curatedPath), dataframeCaptor.capture(), eq(primaryKey));

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.process(spark, expectedRecords, mockSourceReference).collectAsList()
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
        doNothing().when(mockDataStorage).upsertRecords(eq(spark), eq(curatedPath), dataframeCaptor.capture(), eq(primaryKey));

        underTest.process(spark, expectedRecords, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidUpdateRecords() throws DataStorageException {
        val expectedRecords = createStructuredUpdateDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).updateRecords(eq(spark), eq(curatedPath), dataframeCaptor.capture(), eq(primaryKey));

        underTest.process(spark, expectedRecords, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    @Test
    public void shouldHandleValidDeleteRecords() throws DataStorageException {
        val expectedRecords = createStructuredDeleteDataset(spark);

        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).deleteRecords(eq(spark), eq(curatedPath), dataframeCaptor.capture(), eq(primaryKey));

        underTest.process(spark, expectedRecords, mockSourceReference).collect();

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                getAllCapturedRecords(dataframeCaptor)
        );
    }

    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getPrimaryKey()).thenReturn(primaryKey);
    }

}