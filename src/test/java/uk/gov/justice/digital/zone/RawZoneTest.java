package uk.gov.justice.digital.zone;


import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
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
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.zone.Fixtures.*;

@ExtendWith(MockitoExtension.class)
class RawZoneTest extends BaseSparkTest {

    private static final JobArguments jobArguments =
            new JobArguments(Collections.singletonMap(JobArguments.RAW_S3_PATH, RAW_PATH));

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorageService;

    @Mock
    private SourceReferenceService mockSourceReferenceService;

    @Test
    public void processLoadShouldCreateRawLoadRecordsSortedByTimestamp() throws DataStorageException {
        val testDataFrame = createTestRecords();
        val expectedDataFrame = createExpectedLoadRecords();

        val rawPath = createValidatedPath(RAW_PATH, TABLE_SOURCE, TABLE_NAME);

        when(mockSourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME))
                .thenReturn(Optional.of(mockSourceReference));

        doNothing()
                .when(mockDataStorageService)
                .appendDistinct(eq(rawPath), refEq(expectedDataFrame), eq(RawZone.PRIMARY_KEY_NAME));

        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);

        val underTest = new RawZone(
                jobArguments,
                mockDataStorageService,
                mockSourceReferenceService
        );

        assertIterableEquals(
                expectedDataFrame.collectAsList(),
                underTest.processLoad(spark, testDataFrame, dataMigrationEventRow).collectAsList()
        );
    }

    @Test
    public void processCDCShouldCreateCDCRecordsSortedByTimestamp() throws DataStorageException {
        val testDataFrame = createTestRecords();
        val expectedDataFrame = createExpectedCDCRecords();

        val rawPath = createValidatedPath(RAW_PATH, TABLE_SOURCE, TABLE_NAME);

        when(mockSourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME))
                .thenReturn(Optional.of(mockSourceReference));

        doNothing()
                .when(mockDataStorageService)
                .appendDistinct(eq(rawPath), refEq(expectedDataFrame), eq(RawZone.PRIMARY_KEY_NAME));

        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);

        val underTest = new RawZone(
                jobArguments,
                mockDataStorageService,
                mockSourceReferenceService
        );

        assertIterableEquals(
                expectedDataFrame.collectAsList(),
                underTest.processCDC(spark, testDataFrame, dataMigrationEventRow).collectAsList()
        );
    }

    @Test
    public void processLoadShouldSkipProcessingGivenInvalidOperation() throws DataStorageException {
        val testDataFrame = createTestRecords().withColumn(OPERATION, functions.lit("invalidOperation"));
        val underTest = new RawZone(
                jobArguments,
                mockDataStorageService,
                mockSourceReferenceService
        );

        assertTrue(
                underTest
                        .processLoad(spark, testDataFrame, dataMigrationEventRowWithInvalidOperation)
                        .collectAsList()
                        .isEmpty()
        );
    }

    @Test
    public void processCDCShouldSkipProcessingGivenInvalidOperation() throws DataStorageException {
        val testDataFrame = createTestRecords().withColumn(OPERATION, functions.lit("invalidOperation"));
        val underTest = new RawZone(
                jobArguments,
                mockDataStorageService,
                mockSourceReferenceService
        );

        assertTrue(
                underTest
                        .processCDC(spark, testDataFrame, dataMigrationEventRowWithInvalidOperation)
                        .collectAsList()
                        .isEmpty()
        );
    }

    private Dataset<Row> createTestRecords() {
        val rawData = new ArrayList<Row>();
        rawData.add(RowFactory.create("3", "load-record-key1", TABLE_SOURCE, TABLE_NAME, Load.getName(), ROW_CONVERTER, RAW_DATA));
        rawData.add(RowFactory.create("1", "load-record-key2", TABLE_SOURCE, TABLE_NAME, Load.getName(), ROW_CONVERTER, RAW_DATA));
        rawData.add(RowFactory.create("2", "load-record-key3", TABLE_SOURCE, TABLE_NAME, Load.getName(), ROW_CONVERTER, RAW_DATA));
        rawData.add(RowFactory.create("0", "insert-record-key1", TABLE_SOURCE, TABLE_NAME, Insert.getName(), ROW_CONVERTER, RAW_DATA));
        rawData.add(RowFactory.create("4", "insert-record-key2", TABLE_SOURCE, TABLE_NAME, Insert.getName(), ROW_CONVERTER, RAW_DATA));
        rawData.add(RowFactory.create("6", "update-record-key1", TABLE_SOURCE, TABLE_NAME, Update.getName(), ROW_CONVERTER, RAW_DATA));
        rawData.add(RowFactory.create("5", "delete-record-key1", TABLE_SOURCE, TABLE_NAME, Delete.getName(), ROW_CONVERTER, RAW_DATA));

        return spark.createDataFrame(rawData, RECORD_SCHEMA);
    }

    private Dataset<Row> createExpectedLoadRecords() {
        val expectedRawData = new ArrayList<Row>();

        expectedRawData.add(
                RowFactory.create(
                        "load-record-key2:1:" + Load.getName(),
                        "1",
                        "load-record-key2",
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Load.getName(),
                        ROW_CONVERTER,
                        RAW_DATA
                )
        );
        expectedRawData.add(
                RowFactory.create(
                        "load-record-key3:2:" + Load.getName(),
                        "2",
                        "load-record-key3",
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Load.getName(),
                        ROW_CONVERTER,
                        RAW_DATA
                )
        );
        expectedRawData.add(
                RowFactory.create(
                        "load-record-key1:3:" + Load.getName(),
                        "3",
                        "load-record-key1",
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Load.getName(),
                        ROW_CONVERTER,
                        RAW_DATA
                )
        );

        return spark.createDataFrame(expectedRawData, EXPECTED_RAW_SCHEMA);
    }

    private Dataset<Row> createExpectedCDCRecords() {
        val expectedRawData = new ArrayList<Row>();

        expectedRawData.add(
                RowFactory.create(
                        "insert-record-key1:0:" + Insert.getName(),
                        "0",
                        "insert-record-key1",
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Insert.getName(),
                        ROW_CONVERTER,
                        RAW_DATA
                )
        );
        expectedRawData.add(
                RowFactory.create(
                        "insert-record-key2:4:" + Insert.getName(),
                        "4",
                        "insert-record-key2",
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Insert.getName(),
                        ROW_CONVERTER,
                        RAW_DATA
                )
        );
        expectedRawData.add(
                RowFactory.create(
                        "delete-record-key1:5:" + Delete.getName(),
                        "5",
                        "delete-record-key1",
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Delete.getName(),
                        ROW_CONVERTER,
                        RAW_DATA
                )
        );
        expectedRawData.add(
                RowFactory.create(
                        "update-record-key1:6:" + Update.getName(),
                        "6",
                        "update-record-key1",
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Update.getName(),
                        ROW_CONVERTER,
                        RAW_DATA
                )
        );

        return spark.createDataFrame(expectedRawData, EXPECTED_RAW_SCHEMA);
    }
}
