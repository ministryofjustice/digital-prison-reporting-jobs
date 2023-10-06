package uk.gov.justice.digital.service;

import io.delta.tables.DeltaOptimizeBuilder;
import io.delta.tables.DeltaTable;
import lombok.val;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataStorageServiceTest extends BaseSparkTest {

    private static final int DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES = 1;

    private static final DataStorageService underTest = new DataStorageService();


    private MockedStatic<DeltaTable> mockDeltaTableStatic;

    @Mock
    private DeltaTable mockDeltaTable;

    @Mock
    private DeltaOptimizeBuilder mockDeltaOptimize;

    @Mock
    private Dataset<Row> mockDataSet;

    @Mock
    private DataFrameWriter<Row> mockDataFrameWriter;

    @TempDir
    private Path folder;

    private TableIdentifier tableId;

    private String tablePath;

    @BeforeEach
    void setUp() {
        mockDeltaTableStatic = mockStatic(DeltaTable.class);
        tableId = new TableIdentifier(
                folder.toAbsolutePath().toString(),
                "domain",
                "incident",
                "demographics"
        );
        tablePath = tableId.toPath();
    }

    @AfterEach
    void tearDown() {
        mockDeltaTableStatic.close();
    }

    @Test
    public void shouldReturnTrueWhenStorageExists() {
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        assertTrue(underTest.exists(spark, tableId));
    }

    @Test
    public void shouldReturnFalseWhenStorageDoesNotExist() {
        when(DeltaTable.isDeltaTable(spark, tableId.toPath())).thenReturn(false);
        assertFalse(underTest.exists(spark, tableId));
    }

    @Test
    public void shouldReturnTrueForHasRecordsWhenStorageExistsAndRecordsArePresent() throws DataStorageException {
        givenDeltaTableExists();

        val df = spark.sql("select cast(10 as LONG) as numFiles");

        when(mockDeltaTable.toDF()).thenReturn(df);

        assertTrue(underTest.exists(spark, tableId));
        assertTrue(underTest.hasRecords(spark, tableId));
    }

    @Test
    public void shouldReturnFalseForHasRecordsWhenStorageExistsAndRecordsAreNotPresent() throws DataStorageException {
        givenDeltaTableExists();

        val df = spark.emptyDataFrame();

        when(mockDeltaTable.toDF()).thenReturn(df);

        assertTrue(underTest.exists(spark, tableId));
        assertFalse(underTest.hasRecords(spark, tableId));
    }

    @Test
    public void shouldReturnFalseForHasRecordsWhenStorageDoesNotExist() throws DataStorageException {
        assertFalse(underTest.exists(spark, tableId));
        assertFalse(underTest.hasRecords(spark, tableId));
    }

    @Test
    public void shouldAppendCompleteForDeltaTable() throws DataStorageException {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.mode(anyString())).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);

        underTest.append(tablePath, mockDataSet);

        verify(mockDataFrameWriter).mode("append");
        verify(mockDataFrameWriter).save();
    }

    @Test
    public void shouldCreateCompleteForDeltaTable() throws DataStorageException {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);

        underTest.create(tablePath, mockDataSet);

        verify(mockDataFrameWriter).save();
    }

    @Test
    public void shouldReplaceCompleteForDeltaTable() throws DataStorageException {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.mode(anyString())).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyBoolean())).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);

        underTest.replace(tablePath, mockDataSet);

        verify(mockDataFrameWriter).mode("overwrite");
        verify(mockDataFrameWriter).option("overwriteSchema", true);
        verify(mockDataFrameWriter).save();
    }

    @Test
    public void shouldReloadCompleteForDeltaTable() throws DataStorageException {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.mode(anyString())).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);

        underTest.resync(tablePath, mockDataSet);

        verify(mockDataFrameWriter).mode("overwrite");
        verify(mockDataFrameWriter).save();
    }

    @Test
    public void shouldDeleteCompleteForDeltaTable() throws DataStorageException {
        givenDeltaTableExists();
        underTest.delete(spark, tableId);
        verify(mockDeltaTable).delete();
    }

    @Test
    public void shouldVacuumCompleteForDeltaTable() throws DataStorageException {
        givenDeltaTableExists();
        underTest.vacuum(spark, tableId);
        verify(mockDeltaTable).vacuum();
    }

    @Test
    public void shouldVacuumForDeltaTablePath() throws DataStorageException {
        givenDeltaTableExists();
        underTest.vacuum(spark, tableId.toPath());
        verify(mockDeltaTable).vacuum();
    }

    @Test
    public void shouldCompactDeltaTable() throws DataStorageException {
        givenDeltaTableExists();
        when(mockDeltaTable.optimize()).thenReturn(mockDeltaOptimize);

        underTest.compactDeltaTable(spark, tableId.toPath());
        verify(mockDeltaTable).optimize();
        verify(mockDeltaOptimize).executeCompaction();
    }

    @Test
    public void shouldThrowForBadlyFormattedDeltaTablePath() {
        assertThrows(DataStorageException.class, () ->
                underTest.listDeltaTablePaths(spark, "://some-path", DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)
        );
    }

    @Test
    public void shouldThrowWhenDeltaTablePathDoesNotExist() {
        assertThrows(DataStorageException.class, () ->
            underTest.listDeltaTablePaths(spark, "/doesnotexist", DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)
        );
    }

    @Test
    public void shouldGetDeltaTableWhenExists() {
        givenDeltaTableExists();
        underTest.get(spark, tableId);
        verify(mockDeltaTable).toDF();
    }

    @Test
    public void shouldGenerateManifestWhenEndTableUpdatesCalled() throws DataStorageException {
        givenDeltaTableExists();
        underTest.endTableUpdates(spark, tableId);
        verifyManifestGeneratedWithExpectedModeString();
    }

    @Test
    public void shouldUpdateDeltaManifestForTable() {
        givenDeltaTableExists();
        underTest.updateDeltaManifestForTable(spark, tablePath);
        verifyManifestGeneratedWithExpectedModeString();
    }

    private void givenDeltaTableExists() {
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockDeltaTable);
    }

    private void verifyManifestGeneratedWithExpectedModeString() {
        verify(mockDeltaTable).generate("symlink_format_manifest");
    }
}
