package uk.gov.justice.digital.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.SparkTestBase;

import java.io.IOException;
import java.nio.file.Path;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.justice.digital.test.SparkTestHelpers.*;

/**
 * Helper to set up a test environment with some semi-realistic delta tables and extraneous files/dirs on disk
 */
public class DeltaTablesTestBase extends SparkTestBase {
    @TempDir
    protected Path rootPath;
    protected Path offendersTablePath;
    protected Path offenderBookingsTablePath;
    protected Path agencyLocationsTablePathDepth2;
    protected Path internalLocationsTablePathDepth3;


    protected void setupDeltaTablesFixture() {
        SparkTestHelpers helpers = new SparkTestHelpers(spark);
        offendersTablePath = rootPath.resolve("offenders").toAbsolutePath();
        offenderBookingsTablePath = rootPath.resolve("offender-bookings").toAbsolutePath();
        agencyLocationsTablePathDepth2 = rootPath.resolve("another-dir-depth-1").resolve("agency-locations").toAbsolutePath();
        internalLocationsTablePathDepth3 = rootPath.resolve("another-dir-depth-1").resolve("yet-another-dir-depth-2").resolve("internal-locations").toAbsolutePath();
        // repartition to force the data in the delta table to have multiple small files at the start of tests
        int largeNumPartitions = 5;
        Dataset<Row> offenders = helpers.readSampleParquet(OFFENDERS_SAMPLE_PARQUET_PATH).repartition(largeNumPartitions);
        helpers.overwriteDeltaTable(offendersTablePath.toString(), offenders);
        Dataset<Row> offenderBookings = helpers.readSampleParquet(OFFENDER_BOOKINGS_SAMPLE_PARQUET_PATH).repartition(largeNumPartitions);
        helpers.overwriteDeltaTable(offenderBookingsTablePath.toString(), offenderBookings);

        Dataset<Row> agencyLocations = helpers.readSampleParquet(AGENCY_LOCATIONS_SAMPLE_PARQUET_PATH).repartition(largeNumPartitions);
        helpers.overwriteDeltaTable(agencyLocationsTablePathDepth2.toString(), agencyLocations);
        Dataset<Row> internalLocations = helpers.readSampleParquet(INTERNAL_LOCATIONS_SAMPLE_PARQUET_PATH).repartition(largeNumPartitions);
        helpers.overwriteDeltaTable(internalLocationsTablePathDepth3.toString(), internalLocations);
    }

    /**
     * Adds some extraneous non-delta table files and directories in to the root path
     */
    protected void setupNonDeltaFilesAndDirs() throws IOException {
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

    protected static long numberOfCompactions(String tablePath) {
        return spark.sql(format("DESCRIBE HISTORY delta.`%s`", tablePath))
                .where("operation = 'OPTIMIZE'").count();
    }

}
