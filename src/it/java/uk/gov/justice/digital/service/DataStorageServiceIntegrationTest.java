package uk.gov.justice.digital.service;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.test.DeltaTablesTestBase;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.test.SparkTestHelpers.countParquetFiles;

public class DataStorageServiceIntegrationTest extends DeltaTablesTestBase {
    private static final DataStorageService underTest = new DataStorageService();

    @BeforeAll
    public static void setupTest() throws Exception {
        setupDeltaTablesFixture();
        setupNonDeltaFilesAndDirs();
    }
    @Test
    public void shouldListOnlyDeltaTablePaths() throws Exception {
        List<String> deltaTables = underTest.listDeltaTablePaths(spark, rootPath.toString());
        // Should ignore non-delta table directories and files in the rootPath
        assertEquals(2, deltaTables.size());
    }

    @Test
    public void shouldCompactADeltaTable() throws Exception {
        assertMultipleParquetFilesPrecondition(offendersTablePath);
        // Compaction should add a single new parquet file containing all the data from the original files.
        // It won't remove the old parquet files until a vacuum occurs and the data has passed its retention period.
        long originalNumberOfParquetFiles = countParquetFiles(offendersTablePath);
        long expectedNumberOfParquetFilesAfterCompaction = originalNumberOfParquetFiles + 1;

        underTest.compactDeltaTable(spark, offendersTablePath.toString());

        assertEquals(expectedNumberOfParquetFilesAfterCompaction, countParquetFiles(offendersTablePath));
    }

    @Test
    public void shouldVacuumADeltaTable() throws Exception {
        assertMultipleParquetFilesPrecondition(offenderBookingsTablePath);
        // Compaction followed by vacuum on a table with zero retention should result in a single parquet file
        underTest.compactDeltaTable(spark, offenderBookingsTablePath.toString());
        setDeltaTableRetentionToZero(offenderBookingsTablePath.toString());
        underTest.vacuum(spark, offenderBookingsTablePath.toString());

        assertEquals(1, countParquetFiles(offenderBookingsTablePath));
    }
}