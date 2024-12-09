package uk.gov.justice.digital.service;

import io.delta.exceptions.ConcurrentDeleteReadException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.MaintenanceOperationFailedException;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MaintenanceServiceTest {

    private static final String rootPath = "s3:///some/path";
    private static final String table1 = rootPath + "/table1";
    private static final String table2 = rootPath + "/table2";
    private static final String table3 = rootPath + "/table3";
    private static final List<String> deltaTablePaths = Arrays.asList(table1, table2, table3);
    private static final int DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES = 1;

    private MaintenanceService underTest;
    @Mock
    private DataStorageService mockDataStorageService;
    @Mock
    private SparkSession mockSparkSession;

    @BeforeEach
    public void setup() {
        underTest = new MaintenanceService(mockDataStorageService);
    }

    @Test
    void shouldCompactAllTablesWhenNoConfigIsProvided() {
        when(mockDataStorageService.listDeltaTablePaths(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)).thenReturn(deltaTablePaths);
        underTest.compactDeltaTables(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table1);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table2);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table3);
    }

    @Test
    void shouldCompactConfiguredTablesWhenConfigIsProvided() {
        when(mockDataStorageService.listDeltaTablePaths(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)).thenReturn(deltaTablePaths);
        underTest.compactDeltaTables(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table1);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table3);
    }

    @Test
    public void compactShouldListTablePaths() {
        underTest.compactDeltaTables(mockSparkSession, rootPath, 1);
        verify(mockDataStorageService).listDeltaTablePaths(mockSparkSession, rootPath, 1);
        underTest.compactDeltaTables(mockSparkSession, "s3://anotherpath", 2);
        verify(mockDataStorageService).listDeltaTablePaths(mockSparkSession, "s3://anotherpath", 2);
    }

    @Test
    public void shouldTryToCompactAllTablesWhenCompactionFailsWithConcurrentDeleteReadException() {
        when(mockDataStorageService.listDeltaTablePaths(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)).thenReturn(deltaTablePaths);

        // This is a specific Exception we have seen and want to handle
        doThrow(new ConcurrentDeleteReadException("Failed compaction"))
                .when(mockDataStorageService).compactDeltaTable(eq(mockSparkSession), anyString());

        assertThrows(MaintenanceOperationFailedException.class, () -> underTest.compactDeltaTables(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES));
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table1);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table2);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table3);
    }

    @Test
    public void shouldTryToCompactAllTablesWhenCompactionFailsWithException() {
        when(mockDataStorageService.listDeltaTablePaths(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)).thenReturn(deltaTablePaths);

        // make sure we handle checked and unchecked exceptions in general
        doThrow(new DataStorageException("Failed compaction"))
                .when(mockDataStorageService).compactDeltaTable(eq(mockSparkSession), eq(table1));

        doThrow(new RuntimeException(
                "Failed compaction"))
                .when(mockDataStorageService).compactDeltaTable(eq(mockSparkSession), eq(table2));

        assertThrows(MaintenanceOperationFailedException.class, () -> underTest.compactDeltaTables(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES));
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table1);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table2);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table3);
    }

    @Test
    void shouldVacuumAllTablesWhenNoConfigIsProvided() {
        when(mockDataStorageService.listDeltaTablePaths(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)).thenReturn(deltaTablePaths);
        underTest.vacuumDeltaTables(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES);
        verify(mockDataStorageService).vacuum(mockSparkSession, table1);
        verify(mockDataStorageService).vacuum(mockSparkSession, table2);
        verify(mockDataStorageService).vacuum(mockSparkSession, table3);
    }

    @Test
    void shouldVacuumConfiguredTablesWhenConfigIsProvided() {
        when(mockDataStorageService.listDeltaTablePaths(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)).thenReturn(deltaTablePaths);
        underTest.vacuumDeltaTables(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES);
        verify(mockDataStorageService).vacuum(mockSparkSession, table1);
        verify(mockDataStorageService).vacuum(mockSparkSession, table3);
    }

    @Test
    public void vacuumShouldListTablePaths() {
        underTest.vacuumDeltaTables(mockSparkSession, rootPath, 1);
        verify(mockDataStorageService).listDeltaTablePaths(mockSparkSession, rootPath, 1);
        underTest.vacuumDeltaTables(mockSparkSession, "s3://anotherpath", 2);
        verify(mockDataStorageService).listDeltaTablePaths(mockSparkSession, "s3://anotherpath", 2);
    }

    @Test
    public void shouldTryToVacuumAllTablesWhenVacuumFailsWithException() {
        when(mockDataStorageService.listDeltaTablePaths(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)).thenReturn(deltaTablePaths);

        doThrow(new DataStorageException("Failed vacuum"))
                .when(mockDataStorageService).vacuum(eq(mockSparkSession), eq(table1));

        doThrow(new RuntimeException(
                "Failed vacuum"))
                .when(mockDataStorageService).vacuum(eq(mockSparkSession), eq(table2));

        assertThrows(MaintenanceOperationFailedException.class, () -> underTest.vacuumDeltaTables(mockSparkSession, rootPath, DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES));
        verify(mockDataStorageService).vacuum(mockSparkSession, table1);
        verify(mockDataStorageService).vacuum(mockSparkSession, table2);
        verify(mockDataStorageService).vacuum(mockSparkSession, table3);
    }

}