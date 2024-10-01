package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.test.DeltaTablesTestBase;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.justice.digital.test.SparkTestHelpers.countParquetFiles;

class MaintenanceServiceVacuumIntegrationTest extends DeltaTablesTestBase {

    private MaintenanceService underTest;

    @BeforeEach
    public void setupTest() throws Exception {
        setupDeltaTablesFixture();
        setupNonDeltaFilesAndDirs();
        underTest = new MaintenanceService(new DataStorageService(new JobArguments(Collections.emptyMap())));

        assertMultipleParquetFilesPrecondition(offendersTablePath);
        assertMultipleParquetFilesPrecondition(offenderBookingsTablePath);
        assertMultipleParquetFilesPrecondition(agencyLocationsTablePathDepth2);
        assertMultipleParquetFilesPrecondition(internalLocationsTablePathDepth3);

        setDeltaTableRetentionToZero(offendersTablePath.toString());
        setDeltaTableRetentionToZero(offenderBookingsTablePath.toString());
        setDeltaTableRetentionToZero(agencyLocationsTablePathDepth2.toString());
        setDeltaTableRetentionToZero(internalLocationsTablePathDepth3.toString());
    }

    @Test
    public void shouldVacuumDeltaTablesWhenRecursingWithDepth1() throws Exception {
        int depthLimit = 1;
        // Compaction followed by vacuum on a table with zero retention should result in a single parquet file
        underTest.compactDeltaTables(spark, rootPath.toString(), ImmutableSet.of(), depthLimit);
        underTest.vacuumDeltaTables(spark, rootPath.toString(), ImmutableSet.of(), depthLimit);

        // The tables in the root have been compacted
        assertEquals(1, countParquetFiles(offendersTablePath));
        assertEquals(1, countParquetFiles(offenderBookingsTablePath));
        // The tables in subdirectories have not been compacted
        assertTrue(countParquetFiles(agencyLocationsTablePathDepth2) > 1);
        assertTrue(countParquetFiles(internalLocationsTablePathDepth3) > 1);
    }

    @Test
    public void shouldVacuumDeltaTablesWhenRecursingWithDepth2() throws Exception {
        int depthLimit = 2;
        // Compaction followed by vacuum on a table with zero retention should result in a single parquet file
        underTest.compactDeltaTables(spark, rootPath.toString(), ImmutableSet.of(), depthLimit);
        underTest.vacuumDeltaTables(spark, rootPath.toString(), ImmutableSet.of(), depthLimit);

        // The tables in the root and 2nd level have been compacted
        assertEquals(1, countParquetFiles(offendersTablePath));
        assertEquals(1, countParquetFiles(offenderBookingsTablePath));
        assertEquals(1, countParquetFiles(agencyLocationsTablePathDepth2));
        // The tables in subdirectories below depth 2 have not been compacted
        assertTrue(countParquetFiles(internalLocationsTablePathDepth3) > 1);
    }

    @Test
    public void shouldVacuumDeltaTablesWhenRecursingWithDepth3() throws Exception {
        int depthLimit = 3;
        // Compaction followed by vacuum on a table with zero retention should result in a single parquet file
        underTest.compactDeltaTables(spark, rootPath.toString(), ImmutableSet.of(), depthLimit);
        underTest.vacuumDeltaTables(spark, rootPath.toString(), ImmutableSet.of(), depthLimit);

        // The tables have all been compacted down to level 3 subdirectories
        assertEquals(1, countParquetFiles(offendersTablePath));
        assertEquals(1, countParquetFiles(offenderBookingsTablePath));
        assertEquals(1, countParquetFiles(agencyLocationsTablePathDepth2));
        assertEquals(1, countParquetFiles(internalLocationsTablePathDepth3));
    }

}