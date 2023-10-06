package uk.gov.justice.digital.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.test.DeltaTablesTestBase;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.test.SparkTestHelpers.countParquetFiles;

class MaintenanceServiceCompactionIntegrationTest extends DeltaTablesTestBase {

    private MaintenanceService underTest;

    @BeforeEach
    public void setupTest() throws Exception {
        setupDeltaTablesFixture();
        setupNonDeltaFilesAndDirs();
        underTest = new MaintenanceService(new DataStorageService());
    }

    @Test
    public void shouldCompactDeltaTableWhenRecursingDepth1() throws Exception {
        int depthLimit = 1;
        assertMultipleParquetFilesPrecondition(offendersTablePath);
        assertMultipleParquetFilesPrecondition(offenderBookingsTablePath);
        // Compaction should add a single new parquet file containing all the data from the original files.
        // It won't remove the old parquet files until a vacuum occurs and the data has passed its retention period.
        long originalNumberOfParquetFilesOffenders = countParquetFiles(offendersTablePath);
        long originalNumberOfParquetFilesOffenderBookings = countParquetFiles(offenderBookingsTablePath);


        long expectedNumberOfParquetFilesAfterCompactionOffenders = originalNumberOfParquetFilesOffenders + 1;
        long expectedNumberOfParquetFilesAfterCompactionOffenderBookings = originalNumberOfParquetFilesOffenderBookings + 1;

        underTest.compactDeltaTables(spark, rootPath.toString(), depthLimit);

        // In this test we verify compaction using both the effect on number of files and the reported delta operations.
        // In other tests we just check the delta operations in metadata.
        assertEquals(expectedNumberOfParquetFilesAfterCompactionOffenders, countParquetFiles(offendersTablePath));
        assertEquals(expectedNumberOfParquetFilesAfterCompactionOffenderBookings, countParquetFiles(offenderBookingsTablePath));

        assertEquals(1, numberOfCompactions(offendersTablePath.toString()));
        assertEquals(1, numberOfCompactions(offenderBookingsTablePath.toString()));
        assertEquals(0, numberOfCompactions(agencyLocationsTablePath.toString()));
        assertEquals(0, numberOfCompactions(internalLocationsTablePath.toString()));
    }

    @Test
    public void shouldCompactDeltaTablesWhenRecursingDepth2() throws Exception {
        int depthLimit = 2;

        underTest.compactDeltaTables(spark, rootPath.toString(), depthLimit);

        assertEquals(1, numberOfCompactions(offendersTablePath.toString()));
        assertEquals(1, numberOfCompactions(offenderBookingsTablePath.toString()));
        assertEquals(1, numberOfCompactions(agencyLocationsTablePath.toString()));
        assertEquals(0, numberOfCompactions(internalLocationsTablePath.toString()));
    }

    @Test
    public void shouldCompactDeltaTablesWhenRecursingDepth3() throws Exception {
        int depthLimit = 3;

        underTest.compactDeltaTables(spark, rootPath.toString(), depthLimit);

        assertEquals(1, numberOfCompactions(offendersTablePath.toString()));
        assertEquals(1, numberOfCompactions(offenderBookingsTablePath.toString()));
        assertEquals(1, numberOfCompactions(agencyLocationsTablePath.toString()));
        assertEquals(1, numberOfCompactions(internalLocationsTablePath.toString()));
    }
}