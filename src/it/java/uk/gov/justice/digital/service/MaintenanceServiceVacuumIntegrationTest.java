package uk.gov.justice.digital.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.test.DeltaTablesTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.test.SparkTestHelpers.countParquetFiles;

class MaintenanceServiceVacuumIntegrationTest extends DeltaTablesTestBase {

    private MaintenanceService underTest;

    @BeforeEach
    public void setup() {
        underTest = new MaintenanceService(new DataStorageService());
    }

    @Test
    public void shouldVacuumAllDeltaTables() throws Exception {
        assertMultipleParquetFilesPrecondition(offendersTablePath);
        assertMultipleParquetFilesPrecondition(offenderBookingsTablePath);
        // Compaction followed by vacuum on a table with zero retention should result in a single parquet file
        underTest.compactDeltaTables(spark, rootPath.toString());
        setDeltaTableRetentionToZero(offendersTablePath.toString());
        setDeltaTableRetentionToZero(offenderBookingsTablePath.toString());
        underTest.vacuumDeltaTables(spark, rootPath.toString());

        assertEquals(1, countParquetFiles(offendersTablePath));
        assertEquals(1, countParquetFiles(offenderBookingsTablePath));
    }

}