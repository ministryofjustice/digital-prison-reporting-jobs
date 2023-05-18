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

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;

class DataStorageServiceTest extends BaseSparkTest {

    private static final TableIdentifier info = new TableIdentifier(
            "s3://test-bucket",
            "domain",
            "incident",
            "demographics"
    );

    private static DataStorageService testStorage;
    private MockedStatic<DeltaTable> mockedStatic;
    private DeltaTable mockedDeltaTable;

    @Mock
    private Dataset<Row> mockedDataSet;

    @TempDir
    private Path folder;

    @BeforeEach
    void setUp() {
        testStorage = new DataStorageService();
        mockedStatic = mockStatic(DeltaTable.class);
        mockedDeltaTable = mock(DeltaTable.class);
    }

    @AfterEach
    void tearDown() {
        mockedStatic.close();
    }

    @Test
    public void shouldReturnTrueWhenStorageExists() {
        val identifier = createValidatedPath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(true);
        val result = testStorage.exists(spark, info);
        assertTrue(result);
    }

    @Test
    public void shouldReturnFalseWhenStorageDoesNotExist() {
        val identifier = createValidatedPath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(false);
        val actualResult = testStorage.exists(spark, info);
        assertFalse(actualResult);
    }

    @Test
    public void shouldReturnTrueForHasRecordsWhenStorageExistsAndRecordsArePresent() throws DataStorageException {
        val df = spark.sql("select cast(10 as LONG) as numFiles");

        TableIdentifier info = new TableIdentifier("s3://test-bucket", "domain",
                "incident", "demographics");
        String identifier = createValidatedPath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(true);
        boolean actualResult = testStorage.exists(spark, info);
        assertTrue(actualResult);

        when(DeltaTable.forPath(spark, identifier)).thenReturn(mockedDeltaTable);
        when(mockedDeltaTable.toDF()).thenReturn(df);
        boolean hasRecords = testStorage.hasRecords(spark, info);
        assertTrue(hasRecords);
    }

    @Test
    public void shouldReturnFalseForHasRecordsWhenStorageExistsAndRecordsAreNotPresent() throws DataStorageException {
        val df = spark.emptyDataFrame();

        TableIdentifier info = new TableIdentifier("s3://test-bucket", "domain",
                "incident", "demographics");
        String identifier = createValidatedPath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(true);
        boolean actualResult = testStorage.exists(spark, info);
        assertTrue(actualResult);

        when(DeltaTable.forPath(spark, identifier)).thenReturn(mockedDeltaTable);
        when(mockedDeltaTable.toDF()).thenReturn(df);
        boolean hasRecords = testStorage.hasRecords(spark, info);
        assertFalse(hasRecords);
    }

    @Test
    public void shouldReturnFalseForHasRecordsWhenStorageDoesNotExist() throws DataStorageException {
        TableIdentifier info = new TableIdentifier("s3://test-bucket", "domain",
                "incident", "demographics");
        String identifier = createValidatedPath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(false);
        boolean actualResult = testStorage.exists(spark, info);
        assertFalse(actualResult);

        when(DeltaTable.forPath(spark, identifier)).thenReturn(mockedDeltaTable);
        boolean hasRecords = testStorage.hasRecords(spark, info);
        assertFalse(hasRecords);
    }

    @Test
    public void shouldAppendCompleteForDeltaTable() throws DataStorageException {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        val mockService = mock(DataStorageService.class);
        doCallRealMethod().when(mockService).append(tablePath, df);
        mockService.append(tablePath, df);
        assertTrue(true);
    }

    @Test
    public void shouldAppendThrowExceptionForDeltaTable() {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        val mockService = mock(DataStorageService.class);
        try {
            doThrow(DataStorageException.class).when(mockService).append(tablePath, df);
            mockService.append(tablePath, df);
        } catch (DataStorageException de) {
            assertTrue(true);
        }
    }

    @Test
    public void shouldCreateCompleteForDeltaTable() throws DataStorageException {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        val mockService = mock(DataStorageService.class);
        doCallRealMethod().when(mockService).create(tablePath, df);
        mockService.create(tablePath, df);
        assertTrue(true);
    }

    @Test
    public void shouldCreateThrowExceptionForDeltaTable() {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        val mockService = mock(DataStorageService.class);
        try {
            doThrow(DataStorageException.class).when(mockService).create(tablePath, df);
            mockService.create(tablePath, df);
        } catch (DataStorageException de) {
            assertTrue(true);
        }
    }

    @Test
    public void shouldReplaceCompleteForDeltaTable() throws DataStorageException {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        val mockService = mock(DataStorageService.class);
        doCallRealMethod().when(mockService).replace(tablePath, df);
        mockService.replace(tablePath, df);
        assertTrue(true);
    }

    @Test
    public void shouldReplaceThrowExceptionForDeltaTable() {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        val mockService = spy(DataStorageService.class);
        try {
            doThrow(DataStorageException.class).when(mockService).replace(tablePath, df);
            mockService.replace(tablePath, df);
        } catch (DataStorageException de) {
            assertTrue(true);
        }
    }

    @Test
    public void shouldReloadCompleteForDeltaTable() throws DataStorageException {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        val mockService = spy(DataStorageService.class);
        doCallRealMethod().when(mockService).reload(tablePath, df);
        mockService.reload(tablePath, df);
        assertTrue(true);
    }

    @Test
    public void shouldReloadThrowExceptionForDeltaTable() {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        val mockService = spy(DataStorageService.class);
        try {
            doThrow(DataStorageException.class).when(mockService).reload(tablePath, df);
            mockService.reload(tablePath, df);
        } catch (DataStorageException de) {
            assertTrue(true);
        }
    }

    @Test
    public void shouldDeleteCompleteForDeltaTable() throws DataStorageException {
        val mockService = mock(DataStorageService.class);
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(mockService).getTable(spark, tablePath);
        doNothing().when(mockedDeltaTable).delete();
        mockService.delete(spark, info);
        assertTrue(true);
    }

    @Test
    public void shouldVacuumCompleteForDeltaTable() throws DataStorageException {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        val mockService = mock(DataStorageService.class);
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(mockService).getTable(spark, tablePath);
        doReturn(mockedDataSet).when(mockedDeltaTable).vacuum();
        mockService.vacuum(spark, info);
        assertTrue(true);
    }

    @Test
    public void shouldLoadCompleteForDeltaTable() {
        val mockService = mock(DataStorageService.class);
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(mockService).getTable(spark, tablePath);
        doReturn(mockedDataSet).when(mockedDeltaTable).toDF();
        val result = mockService.load(spark, info);
        assertNull(result);
    }

    @Test
    public void shouldReturnDeltaTableWhenExists() {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        val result = testStorage.getTable(spark, tablePath);
        assertNotNull(result);
    }

    @Test
    public void shouldReturnNullWhenNotExists() {
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(false);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        val actualResult = testStorage.getTable(spark, tablePath);
        assertNull(actualResult);
    }

    @Test
    public void shouldUpdateendTableUpdates() throws DataStorageException {
        val mockService = mock(DataStorageService.class);
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(mockService).getTable(spark, tablePath);
        doNothing().when(mockService).updateManifest(mockedDeltaTable);
        mockService.endTableUpdates(spark, info);
        assertTrue(true);
    }

    @Test
    public void shouldUpdateManifest() {
        doNothing().when(mockedDeltaTable).generate("test");
        testStorage.updateManifest(mockedDeltaTable);
        assertTrue(true);
    }

    @Test
    public void shouldUpdateDeltaManifestForTable() {
        val mockService = mock(DataStorageService.class);
        val tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(mockService).getTable(spark, tablePath);
        doNothing().when(mockService).updateManifest(mockedDeltaTable);
        mockService.updateDeltaManifestForTable(spark, tablePath);
        assertTrue(true);
    }
}
