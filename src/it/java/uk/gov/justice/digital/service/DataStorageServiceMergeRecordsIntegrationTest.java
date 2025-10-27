package uk.gov.justice.digital.service;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.test.BaseMinimalDataIntegrationTest;

import java.util.Arrays;

import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;

@ExtendWith(MockitoExtension.class)
class DataStorageServiceMergeRecordsIntegrationTest extends BaseMinimalDataIntegrationTest {

    private DataStorageService underTest;

    @Mock
    private JobArguments arguments;

    private String tablePath;

    @BeforeEach
    void setUp() {
        givenRetrySettingsAreConfigured(arguments);
        givenPathIsConfigured();
        underTest = new DataStorageService(arguments);
    }

    @Test
    void shouldInsertDataWhenNoTableExists() {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, "2023-11-13 10:50:00.123456", Insert, "data2")
        ), TEST_DATA_SCHEMA);

        underTest.mergeRecords(spark, tablePath, input, PRIMARY_KEY);

        assertDeltaTableContainsForPK(tablePath, "data1", pk1);
        assertDeltaTableContainsForPK(tablePath, "data2", pk2);
    }

    @Test
    void shouldInsertDataWhenTableExists() {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1a"),
                createRow(pk2, "2023-11-13 10:50:00.123456", Insert, "data2a")
        ), TEST_DATA_SCHEMA);

        createTable(input.schema());

        underTest.mergeRecords(spark, tablePath, input, PRIMARY_KEY);

        assertDeltaTableContainsForPK(tablePath, "data1a", pk1);
        assertDeltaTableContainsForPK(tablePath, "data2a", pk2);
    }

    @Test
    void shouldInsertUpdateAndDeleteData() {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1a"),
                createRow(pk2, "2023-11-13 10:50:00.123456", Insert, "data2a")
        ), TEST_DATA_SCHEMA);

        underTest.mergeRecords(spark, tablePath, input, PRIMARY_KEY);

        assertDeltaTableContainsForPK(tablePath, "data1a", pk1);
        assertDeltaTableContainsForPK(tablePath, "data2a", pk2);

        Dataset<Row> input2 = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:51:00.123456", Update, "data1b"),
                createRow(pk2, "2023-11-13 10:51:00.123456", Delete, "data2b")
        ), TEST_DATA_SCHEMA);

        underTest.mergeRecords(spark, tablePath, input2, PRIMARY_KEY);

        assertDeltaTableContainsForPK(tablePath, "data1b", pk1);
        assertDeltaTableDoesNotContainPK(tablePath, pk2);
    }

    @Test
    void shouldOverwriteDataWhenGettingNewInsertsForExistingKey() {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1a"),
                createRow(pk2, "2023-11-13 10:50:00.123456", Insert, "data2a")
        ), TEST_DATA_SCHEMA);

        underTest.mergeRecords(spark, tablePath, input, PRIMARY_KEY);

        assertDeltaTableContainsForPK(tablePath, "data1a", pk1);
        assertDeltaTableContainsForPK(tablePath, "data2a", pk2);

        Dataset<Row> input2 = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:51:00.123456", Insert, "data1b"),
                createRow(pk2, "2023-11-13 10:51:00.123456", Insert, "data2b")
        ), TEST_DATA_SCHEMA);

        underTest.mergeRecords(spark, tablePath, input2, PRIMARY_KEY);

        assertDeltaTableContainsForPK(tablePath, "data1b", pk1);
        assertDeltaTableContainsForPK(tablePath, "data2b", pk2);
    }

    @Test
    void shouldInsertAnUpdateThatDoesntExistButNotADelete() {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Update, "data1"),
                createRow(pk2, "2023-11-13 10:50:00.123456", Delete, "data2")
        ), TEST_DATA_SCHEMA);

        underTest.mergeRecords(spark, tablePath, input, PRIMARY_KEY);

        assertDeltaTableContainsForPK(tablePath, "data1", pk1);

        assertDeltaTableDoesNotContainPK(tablePath, pk2);
    }

    private void givenPathIsConfigured() {
        tablePath = testRoot.resolve("my-table-path").toAbsolutePath().toString();
    }

    private void createTable(StructType schema) {
        DeltaTable
                .createIfNotExists(spark)
                .addColumns(schema)
                .location(tablePath)
                .execute();
    }
}
