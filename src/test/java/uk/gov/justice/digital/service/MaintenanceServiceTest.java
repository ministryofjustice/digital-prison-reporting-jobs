package uk.gov.justice.digital.service;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MaintenanceServiceTest {

    private static final String rootPath = "s3:///some/path";
    private static final String table1 = rootPath + "/table1";
    private static final String table2 = rootPath + "/table2";
    private static final String table3 = rootPath + "/table3";
    private static final List<String> deltaTablePaths = Arrays.asList(table1, table2, table3);

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
    public void shouldCompactEachTable() throws Exception {
        when(mockDataStorageService.listDeltaTablePaths(mockSparkSession, rootPath)).thenReturn(deltaTablePaths);
        underTest.compactDeltaTables(mockSparkSession, rootPath);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table1);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table2);
        verify(mockDataStorageService).compactDeltaTable(mockSparkSession, table3);
    }

    @Test
    public void shouldVacuumEachTable() throws Exception {
        when(mockDataStorageService.listDeltaTablePaths(mockSparkSession, rootPath)).thenReturn(deltaTablePaths);
        underTest.vacuumDeltaTables(mockSparkSession, rootPath);
        verify(mockDataStorageService).vacuum(mockSparkSession, table1);
        verify(mockDataStorageService).vacuum(mockSparkSession, table2);
        verify(mockDataStorageService).vacuum(mockSparkSession, table3);
    }

}