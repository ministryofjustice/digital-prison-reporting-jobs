package uk.gov.justice.digital.zone;


import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.ArrayList;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.Delete;
import static uk.gov.justice.digital.zone.Fixtures.*;

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

    private StructuredZone underTest;

    private final Dataset<Row> testDataSet = createTestDataset();

    @BeforeEach
    public void setUp() {
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
        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.processLoad(spark, testDataSet, dataMigrationEventRow).collectAsList()
        );
    }

    @Test
    public void shouldHandleValidIncrementalRecords() throws DataStorageException {
        val expectedRecords = createExpectedIncrementalDataset();
        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.processCDC(spark, testDataSet, dataMigrationEventRow).collectAsList()
        );
    }

    @Test
    public void shouldHandleInvalidRecords() throws DataStorageException {
        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        assertNotNull(underTest.processLoad(spark, testDataSet, dataMigrationEventRow));
    }

    @Test
    public void shouldHandleNoSchemaFound() throws DataStorageException {
        givenTheSchemaDoesNotExist();
        assertNotNull(underTest.processLoad(spark, testDataSet, dataMigrationEventRow));
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
    }

    private Dataset<Row> createTestDataset() {
        val rawData = new ArrayList<Row>();

        rawData.add(
                RowFactory.create(
                        "3",
                        RECORD_KEY_1,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Load.getName(),
                        ROW_CONVERTER,
                        recordData1,
                        recordData1,
                        GENERIC_METADATA
                )
        );
        rawData.add(
                RowFactory.create(
                        "1",
                        RECORD_KEY_2,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Load.getName(),
                        ROW_CONVERTER,
                        recordData2,
                        recordData2,
                        GENERIC_METADATA
                )
        );
        rawData.add(
                RowFactory.create(
                        "2",
                        RECORD_KEY_3,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Load.getName(),
                        ROW_CONVERTER,
                        recordData3,
                        recordData3,
                        GENERIC_METADATA
                )
        );
        rawData.add(
                RowFactory.create(
                        "0",
                        RECORD_KEY_4,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Insert.getName(),
                        ROW_CONVERTER,
                        recordData4,
                        recordData4,
                        GENERIC_METADATA
                )
        );
        rawData.add(
                RowFactory.create(
                        "4",
                        RECORD_KEY_5,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Insert.getName(),
                        ROW_CONVERTER,
                        recordData5,
                        recordData5,
                        GENERIC_METADATA
                )
        );
        rawData.add(
                RowFactory.create(
                        "5",
                        RECORD_KEY_6,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Insert.getName(),
                        ROW_CONVERTER,
                        recordData6,
                        recordData6,
                        GENERIC_METADATA
                )
        );
        rawData.add(
                RowFactory.create(
                        "6",
                        RECORD_KEY_7,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Update.getName(),
                        ROW_CONVERTER,
                        recordData7,
                        recordData7,
                        GENERIC_METADATA
                )
        );
        rawData.add(
                RowFactory.create(
                        "7",
                        RECORD_KEY_6,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Delete.getName(),
                        ROW_CONVERTER,
                        recordData6,
                        recordData6,
                        GENERIC_METADATA
                )
        );
        rawData.add(
                RowFactory.create(
                        "8",
                        RECORD_KEY_5,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Update.getName(),
                        ROW_CONVERTER,
                        recordData5,
                        recordData5,
                        GENERIC_METADATA
                )
        );

        return spark.createDataFrame(rawData, ROW_SCHEMA);
    }

    private Dataset<Row> createExpectedLoadDataset() {
        val expectedLoadData = new ArrayList<Row>();

        expectedLoadData.add(
                RowFactory.create(
                        RECORD_KEY_2,
                        STRING_FIELD_VALUE,
                        null,
                        NUMBER_FIELD_VALUE,
                        ARRAY_FIELD_VALUE,
                        Load.getName()
                )
        );
        expectedLoadData.add(
                RowFactory.create(
                        RECORD_KEY_3,
                        STRING_FIELD_VALUE,
                        null,
                        NUMBER_FIELD_VALUE,
                        ARRAY_FIELD_VALUE,
                        Load.getName()
                )
        );
        expectedLoadData.add(
                RowFactory.create(
                        RECORD_KEY_1,
                        STRING_FIELD_VALUE,
                        null,
                        NUMBER_FIELD_VALUE,
                        ARRAY_FIELD_VALUE,
                        Load.getName()
                )
        );

        return spark.createDataFrame(expectedLoadData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    private Dataset<Row> createExpectedIncrementalDataset() {
        val expectedIncrementalData = new ArrayList<Row>();

        expectedIncrementalData.add(
                RowFactory.create(
                        RECORD_KEY_4,
                        STRING_FIELD_VALUE,
                        null,
                        NUMBER_FIELD_VALUE,
                        ARRAY_FIELD_VALUE,
                        Insert.getName()
                )
        );
        expectedIncrementalData.add(
                RowFactory.create(
                        RECORD_KEY_5,
                        STRING_FIELD_VALUE,
                        null,
                        NUMBER_FIELD_VALUE,
                        ARRAY_FIELD_VALUE,
                        Insert.getName()
                )
        );
        expectedIncrementalData.add(
                RowFactory.create(
                        RECORD_KEY_6,
                        STRING_FIELD_VALUE,
                        null,
                        NUMBER_FIELD_VALUE,
                        ARRAY_FIELD_VALUE,
                        Insert.getName()
                )
        );
        expectedIncrementalData.add(
                RowFactory.create(
                        RECORD_KEY_7,
                        STRING_FIELD_VALUE,
                        null,
                        NUMBER_FIELD_VALUE,
                        ARRAY_FIELD_VALUE,
                        Update.getName()
                )
        );
        expectedIncrementalData.add(
                RowFactory.create(
                        RECORD_KEY_6,
                        STRING_FIELD_VALUE,
                        null,
                        NUMBER_FIELD_VALUE,
                        ARRAY_FIELD_VALUE,
                        Delete.getName()
                )
        );
        expectedIncrementalData.add(
                RowFactory.create(
                        RECORD_KEY_5,
                        STRING_FIELD_VALUE,
                        null,
                        NUMBER_FIELD_VALUE,
                        ARRAY_FIELD_VALUE,
                        Update.getName()
                )
        );

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

}