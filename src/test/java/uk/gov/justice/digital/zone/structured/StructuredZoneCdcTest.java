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
import uk.gov.justice.digital.service.DataStorageService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.test.Fixtures.*;
import static uk.gov.justice.digital.test.ZoneFixtures.*;

@ExtendWith(MockitoExtension.class)
class StructuredZoneCdcTest extends BaseSparkTest {

    @Mock
    private JobArguments mockJobArguments;

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorage;

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
                mockDataStorage
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

    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getSchema()).thenReturn(JSON_DATA_SCHEMA);
        when(mockSourceReference.getPrimaryKey()).thenReturn(primaryKey);
    }

}