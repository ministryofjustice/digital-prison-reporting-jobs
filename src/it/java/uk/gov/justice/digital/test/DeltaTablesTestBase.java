package uk.gov.justice.digital.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.BaseSparkTest;

import java.io.IOException;
import java.nio.file.Path;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.justice.digital.test.SparkTestHelpers.*;

/**
 * Sets up a test environment with some semi-realistic delta tables and extraneous files/dirs on disk
 */
public class DeltaTablesTestBase extends BaseSparkTest {
    @TempDir
    protected static Path rootPath;
    protected static Path offendersTablePath;
    protected static Path offenderBookingsTablePath;

    @BeforeAll
    public static void setupDeltaTables() throws IOException {
        SparkTestHelpers helpers = new SparkTestHelpers(spark);
        // Using offenders data is an arbitrary choice here to provide some test data
        offendersTablePath = rootPath.resolve("offenders").toAbsolutePath();
        offenderBookingsTablePath = rootPath.resolve("offender-bookings").toAbsolutePath();
        // repartition to force the data in the delta table to have multiple small files at the start of the test
        int largeNumPartitions = 5;
        Dataset<Row> offenders = helpers.readSampleParquet(OFFENDERS_SAMPLE_PARQUET_PATH).repartition(largeNumPartitions);
        helpers.overwriteDeltaTable(offendersTablePath.toString(), offenders);
        Dataset<Row> offenderBookings = helpers.readSampleParquet(OFFENDER_BOOKINGS_SAMPLE_PARQUET_PATH).repartition(largeNumPartitions);
        helpers.overwriteDeltaTable(offenderBookingsTablePath.toString(), offenderBookings);

        // Create some files and directories that should be ignored
        assertTrue(rootPath.resolve("file-to-be-ignored.parquet").toFile().createNewFile());
        assertTrue(rootPath.resolve("dir-to-be-ignored").toFile().mkdirs());
        assertTrue(rootPath.resolve("dir-to-be-ignored").resolve("file-to-be-ignored2.parquet").toFile().createNewFile());
    }

    protected void setDeltaTableRetentionToZero(String tablePath) {
        String sql = format(
                "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 seconds')",
                tablePath
        );
        spark.sql(sql).collect();
    }

    protected void assertMultipleParquetFilesPrecondition(Path path) throws IOException {
        assertTrue(
                countParquetFiles(path) > 1,
                "Test pre-condition failed - we want to start with multiple parquet files in this test"
        );
    }

}
