package uk.gov.justice.digital.service;

import io.delta.tables.DeltaTable;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DataStorageServiceTest extends BaseSparkTest {

    private static final TableIdentifier tableId = new TableIdentifier(
            "s3://test-bucket",
            "domain",
            "incident",
            "demographics"
    );

    private static final String tablePath = tableId.toPath();

    private final DataStorageService underTest = new DataStorageService(new SparkSessionProvider());

    private MockedStatic<DeltaTable> mockedStatic;
    private DeltaTable mockedDeltaTable;

    @Mock
    private Dataset<Row> mockedDataSet;

    @TempDir
    private Path folder;

    @BeforeEach
    void setUp() {
        mockedStatic = mockStatic(DeltaTable.class);
        mockedDeltaTable = mock(DeltaTable.class);
    }

    @AfterEach
    void tearDown() {
        mockedStatic.close();
    }

    @Test
    public void shouldReturnTrueWhenStorageExists() {
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        assertTrue(underTest.exists(tableId));
    }

    @Test
    public void shouldReturnFalseWhenStorageDoesNotExist() {
        when(DeltaTable.isDeltaTable(spark, tableId.toPath())).thenReturn(false);
        assertFalse(underTest.exists(tableId));
    }

    @Test
    public void shouldReturnTrueForHasRecordsWhenStorageExistsAndRecordsArePresent() throws DataStorageException {
        val df = spark.sql("select cast(10 as LONG) as numFiles");

        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        assertTrue(underTest.exists(tableId));

        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        when(mockedDeltaTable.toDF()).thenReturn(df);
        assertTrue(underTest.hasRecords(tableId));
    }

    @Test
    public void shouldReturnFalseForHasRecordsWhenStorageExistsAndRecordsAreNotPresent() throws DataStorageException {
        val df = spark.emptyDataFrame();

        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        assertTrue(underTest.exists(tableId));

        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        when(mockedDeltaTable.toDF()).thenReturn(df);
        assertFalse(underTest.hasRecords(tableId));
    }

    @Test
    public void shouldReturnFalseForHasRecordsWhenStorageDoesNotExist() throws DataStorageException {
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(false);
        assertFalse(underTest.exists(tableId));

        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        assertFalse(underTest.hasRecords(tableId));
    }

    // TODO - this test takes 30s
    @Test
    public void shouldAppendCompleteForDeltaTable() throws DataStorageException {
        val tablePath = folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        underTest.append(tablePath, df);
        // TODO - is there anything we can assert on?
        assertTrue(true);
    }

    @Test
    public void shouldCreateCompleteForDeltaTable() throws DataStorageException {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        underTest.create(tablePath, df);
        // TODO - is there anything we can assert on?
        assertTrue(true);
    }

    @Test
    public void shouldReplaceCompleteForDeltaTable() throws DataStorageException {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        underTest.replace(tablePath, df);
        // TODO - is there anything we can assert on?
        assertTrue(true);
    }

    @Test
    public void shouldReloadCompleteForDeltaTable() throws DataStorageException {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        underTest.resync(tablePath, df);
        // TODO - is there anything we can assert on?
        assertTrue(true);
    }

    @Test
    public void shouldDeleteCompleteForDeltaTable() throws DataStorageException {
        val info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, info.toPath())).thenReturn(true);
        when(DeltaTable.forPath(spark, info.toPath())).thenReturn(mockedDeltaTable);
        underTest.delete(info);
        // TODO - is there anything we can assert on?
        assertTrue(true);
    }

    @Test
    public void shouldVacuumCompleteForDeltaTable() throws DataStorageException {
        val info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, info.toPath())).thenReturn(true);
        when(DeltaTable.forPath(spark, info.toPath())).thenReturn(mockedDeltaTable);
        underTest.vacuum(info);
        // TODO - is there anything we can assert on?
        assertTrue(true);
    }

    @Test
    public void shouldLoadCompleteForDeltaTable() {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDataSet).when(mockedDeltaTable).toDF();
        // TODO - review this
        assertNull(underTest.get(info));
    }

    @Test
    public void shouldReturnDeltaTableWhenExists() {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        val result = underTest.getTable(tablePath);
        assertNotNull(result);
    }

    @Test
    public void shouldReturnNullWhenNotExists() {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(false);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        val actualResult = underTest.getTable(tablePath);
        assertNull(actualResult);
    }

    @Test
    public void shouldUpdateendTableUpdates() {
        val info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");

        when(DeltaTable.isDeltaTable(spark, info.toPath())).thenReturn(true);
        when(DeltaTable.forPath(spark, info.toPath())).thenReturn(mockedDeltaTable);

        doNothing().when(mockedDeltaTable).generate("symlink_format_manifest");

        underTest.endTableUpdates(info);
        // TODO - is there anything we can assert on?
        assertTrue(true);
    }

    @Test
    public void shouldUpdateManifest() {
        doNothing().when(mockedDeltaTable).generate("test");
        underTest.updateManifest(mockedDeltaTable);
        // TODO - is there anything we can assert on?
        assertTrue(true);
    }

    @Test
    public void shouldUpdateDeltaManifestForTable() {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        underTest.updateDeltaManifestForTable(tablePath);
        // TODO - is there anything we can assert on?
        assertTrue(true);
    }
}
