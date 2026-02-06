package uk.gov.justice.digital.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.test.DeltaTablesTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.test.SparkTestHelpers.countParquetFiles;

@ExtendWith(MockitoExtension.class)
class MaintenanceServiceCompactionIntegrationTest extends DeltaTablesTestBase {

    @Mock
    private JobArguments arguments;
    @Mock
    private JobProperties properties;

    private MaintenanceService underTest;

    @BeforeEach
    void setupTest() throws Exception {
        setupDeltaTablesFixture();
        setupNonDeltaFilesAndDirs();
        givenRetrySettingsAreConfigured(arguments);
        givenParquetPartitionSettingsAreConfigured(arguments, properties);
        underTest = new MaintenanceService(new DataStorageService(arguments, properties));
    }

    @Test
    void shouldCompactDeltaTableWhenRecursingWithDepth1() throws Exception {
        int depthLimit = 1;
        assertMultipleParquetFilesPrecondition(offendersTablePath);
        assertMultipleParquetFilesPrecondition(offenderBookingsTablePath);
        assertMultipleParquetFilesPrecondition(agencyLocationsTablePathDepth2);
        assertMultipleParquetFilesPrecondition(internalLocationsTablePathDepth3);
        // Compaction should add a single new parquet file containing all the data from the original files.
        // It won't remove the old parquet files until a vacuum occurs and the data has passed its retention period.
        long originalNumFilesOffenders = countParquetFiles(offendersTablePath);
        long originalNumFilesOffenderBookings = countParquetFiles(offenderBookingsTablePath);
        long originalNumFilesAgencyLocations = countParquetFiles(agencyLocationsTablePathDepth2);
        long originalNumFilesInternalLocations = countParquetFiles(internalLocationsTablePathDepth3);

        underTest.compactDeltaTables(spark, rootPath.toString(), depthLimit);

        // In this test we verify compaction using both the effect on number of files and the reported delta operations.
        // In other tests we just check the delta operations in metadata.
        long expectedNumFilesAfterOffenders = originalNumFilesOffenders + 1;
        long expectedNumbFilesAfterOffenderBookings = originalNumFilesOffenderBookings + 1;

        assertEquals(expectedNumFilesAfterOffenders, countParquetFiles(offendersTablePath));
        assertEquals(expectedNumbFilesAfterOffenderBookings, countParquetFiles(offenderBookingsTablePath));
        assertEquals(originalNumFilesAgencyLocations, countParquetFiles(agencyLocationsTablePathDepth2));
        assertEquals(originalNumFilesInternalLocations, countParquetFiles(internalLocationsTablePathDepth3));

        assertEquals(1, numberOfCompactions(offendersTablePath.toString()));
        assertEquals(1, numberOfCompactions(offenderBookingsTablePath.toString()));
        assertEquals(0, numberOfCompactions(agencyLocationsTablePathDepth2.toString()));
        assertEquals(0, numberOfCompactions(internalLocationsTablePathDepth3.toString()));
    }

    @Test
    void shouldCompactDeltaTablesWhenRecursingWithDepth2() {
        int depthLimit = 2;

        underTest.compactDeltaTables(spark, rootPath.toString(), depthLimit);

        assertEquals(1, numberOfCompactions(offendersTablePath.toString()));
        assertEquals(1, numberOfCompactions(offenderBookingsTablePath.toString()));
        assertEquals(1, numberOfCompactions(agencyLocationsTablePathDepth2.toString()));
        assertEquals(0, numberOfCompactions(internalLocationsTablePathDepth3.toString()));
    }

    @Test
    void shouldCompactDeltaTablesWhenRecursingWithDepth3() {
        int depthLimit = 3;

        underTest.compactDeltaTables(spark, rootPath.toString(), depthLimit);

        assertEquals(1, numberOfCompactions(offendersTablePath.toString()));
        assertEquals(1, numberOfCompactions(offenderBookingsTablePath.toString()));
        assertEquals(1, numberOfCompactions(agencyLocationsTablePathDepth2.toString()));
        assertEquals(1, numberOfCompactions(internalLocationsTablePathDepth3.toString()));
    }
}
