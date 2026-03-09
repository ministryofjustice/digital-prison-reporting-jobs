package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.datahub.model.SourceReference;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.MinimalTestData.rowPerPkDfSameTimestamp;

class CdcBatchProcessorLatestRecordsTest extends SparkTestBase {

    private static final SourceReference.PrimaryKey primaryKey = new SourceReference.PrimaryKey("pk");

    @Test
    void shouldNotModifyRecordsForDifferentPKs() {
        Dataset<Row> inputDf = rowPerPkDfSameTimestamp(spark);
        List<Row> expected = inputDf.collectAsList();

        List<Row> result = CdcBatchProcessor.latestRecords(inputDf, primaryKey).collectAsList();

        assertEquals(expected.size(), result.size());
        assertTrue(expected.containsAll(result));
        assertTrue(result.containsAll(expected));
    }

    @Test
    void shouldTakeTheLatestRecordByPKUsingCheckpointCol() {
        Dataset<Row> inputDf = spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1b", "20260205124524000000000000050700865"),
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1a", "20260205124524000000000000050700869"),
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1c", "20260205124524000000000000050700123"),
                createRow(2, "2023-11-13 10:49:28.123456", Insert, "2a", "20260205124524000000000000050700456")
        ), TEST_DATA_SCHEMA);
        List<Row> expected = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1a", "20260205124524000000000000050700869"),
                createRow(2, "2023-11-13 10:49:28.123456", Insert, "2a", "20260205124524000000000000050700456")
        );

        List<Row> result = CdcBatchProcessor.latestRecords(inputDf, primaryKey).collectAsList();

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    void shouldUseTimestampAsATieBreakerForEqualCheckpointCol() {
        // This case should only occur if the checkpoint column is null or empty
        Dataset<Row> inputDf = spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1b", ""),
                createRow(1, "2023-11-13 10:49:27.123456", Update, "1a", ""),
                createRow(1, "2023-11-13 10:49:26.123456", Update, "1c", ""),
                createRow(2, "2023-11-13 10:49:28.123456", Insert, "2a", null),
                createRow(2, "2023-11-13 10:49:29.123456", Insert, "2b", null)
        ), TEST_DATA_SCHEMA);
        List<Row> expected = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1b", ""),
                createRow(2, "2023-11-13 10:49:29.123456", Insert, "2b", null)
        );

        List<Row> result = CdcBatchProcessor.latestRecords(inputDf, primaryKey).collectAsList();

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    void shouldTakeTheLatestRecordByPKForEmptyStringCheckpointCol() {
        // DMS puts an empty string in the checkpoint column (AR_H_CHANGE_SEQ) during a full load
        Dataset<Row> inputDf = spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1b", ""),
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1a", "20260205124524000000000000050700869"),
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1c", "20260205124524000000000000050700123"),
                createRow(2, "2023-11-13 10:49:28.123456", Insert, "2a", "")
        ), TEST_DATA_SCHEMA);
        List<Row> expected = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1a", "20260205124524000000000000050700869"),
                createRow(2, "2023-11-13 10:49:28.123456", Insert, "2a", "")
        );

        List<Row> result = CdcBatchProcessor.latestRecords(inputDf, primaryKey).collectAsList();

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    void shouldTakeTheLatestRecordByPKForNullCheckpointCol() {
        // We shouldn't get nulls in this column, but we explicitly define them as older
        Dataset<Row> inputDf = spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1b", null),
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1a", "20260205124524000000000000050700869"),
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1c", null),
                createRow(2, "2023-11-13 10:49:28.123456", Insert, "2a", null)
        ), TEST_DATA_SCHEMA);
        List<Row> expected = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123456", Update, "1a", "20260205124524000000000000050700869"),
                createRow(2, "2023-11-13 10:49:28.123456", Insert, "2a", null)
        );

        List<Row> result = CdcBatchProcessor.latestRecords(inputDf, primaryKey).collectAsList();

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }
}
