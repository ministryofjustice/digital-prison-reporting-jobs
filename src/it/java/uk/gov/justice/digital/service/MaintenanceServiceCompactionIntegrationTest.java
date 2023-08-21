package uk.gov.justice.digital.service;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.test.DeltaTablesTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.test.SparkTestHelpers.countParquetFiles;

class MaintenanceServiceCompactionIntegrationTest extends DeltaTablesTestBase {

    private static MaintenanceService underTest;

    @BeforeAll
    public static void setupTest() throws Exception {
        setupDeltaTablesFixture();
        setupNonDeltaFilesAndDirs();
        underTest = new MaintenanceService(new DataStorageService());
    }

    @Test
    public void shouldCompactAllDeltaTables() throws Exception {
        assertMultipleParquetFilesPrecondition(offendersTablePath);
        assertMultipleParquetFilesPrecondition(offenderBookingsTablePath);
        // Compaction should add a single new parquet file containing all the data from the original files.
        // It won't remove the old parquet files until a vacuum occurs and the data has passed its retention period.
        long originalNumberOfParquetFilesOffenders = countParquetFiles(offendersTablePath);
        long originalNumberOfParquetFilesOffenderBookings = countParquetFiles(offenderBookingsTablePath);

        long expectedNumberOfParquetFilesAfterCompactionOffenders = originalNumberOfParquetFilesOffenders + 1;
        long expectedNumberOfParquetFilesAfterCompactionOffenderBookings = originalNumberOfParquetFilesOffenderBookings + 1;

        underTest.compactDeltaTables(spark, rootPath.toString());

        assertEquals(expectedNumberOfParquetFilesAfterCompactionOffenders, countParquetFiles(offendersTablePath));
        assertEquals(expectedNumberOfParquetFilesAfterCompactionOffenderBookings, countParquetFiles(offenderBookingsTablePath));
    }
}