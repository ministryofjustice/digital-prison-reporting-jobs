package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.datahub.model.SourceReference;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkDfSameTimestamp;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkDfSameTimestampToMicroSecondAccuracy;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkSameTimestampLatest;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkSameTimestampToMicroSecondAccuracyLatest;
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
    void shouldTakeTheLatestRecordByPK() {
        Dataset<Row> inputDf = manyRowsPerPkDfSameTimestamp(spark);
        List<Row> expected = manyRowsPerPkSameTimestampLatest();

        List<Row> result = CdcBatchProcessor.latestRecords(inputDf, primaryKey).collectAsList();

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    void shouldTakeTheLatestRecordByPKToMicrosecondAccuracy() {
        Dataset<Row> inputDf = manyRowsPerPkDfSameTimestampToMicroSecondAccuracy(spark);
        List<Row> expected = manyRowsPerPkSameTimestampToMicroSecondAccuracyLatest();

        List<Row> result = CdcBatchProcessor.latestRecords(inputDf, primaryKey).collectAsList();

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }
}
