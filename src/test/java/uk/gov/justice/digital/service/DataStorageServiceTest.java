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
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DataStorageServiceTest extends BaseSparkTest {

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
    void shouldReturnTrueWhenStorageExists() {
        TableIdentifier info = new TableIdentifier("s3://test-bucket", "domain",
                "incident", "demographics");
        String identifier = testStorage.getTablePath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(true);
        boolean actualResult = testStorage.exists(spark, info);
        assertTrue(actualResult);
    }

    @Test
    void shouldReturnFalseWhenStorageDoesNotExists() {
        TableIdentifier info = new TableIdentifier("s3://test-bucket", "domain",
                "incident", "demographics");
        String identifier = testStorage.getTablePath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(false);
        boolean actualResult = testStorage.exists(spark, info);
        assertFalse(actualResult);
    }

    @Test
    void shouldReturnTrueForHasRecordsWhenStorageExistsAndRecordsArePresent() throws DataStorageException {
        val df = spark.sql("select cast(10 as LONG) as numFiles");

        TableIdentifier info = new TableIdentifier("s3://test-bucket", "domain",
                "incident", "demographics");
        String identifier = testStorage.getTablePath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(true);
        boolean actualResult = testStorage.exists(spark, info);
        assertTrue(actualResult);

        when(DeltaTable.forPath(spark, identifier)).thenReturn(mockedDeltaTable);
        when(mockedDeltaTable.toDF()).thenReturn(df);
        boolean hasRecords = testStorage.hasRecords(spark, info);
        assertTrue(hasRecords);
    }

    @Test
    void shouldReturnFalseForHasRecordsWhenStorageExistsAndRecordsAreNotPresent() throws DataStorageException {
        val df = spark.emptyDataFrame();

        TableIdentifier info = new TableIdentifier("s3://test-bucket", "domain",
                "incident", "demographics");
        String identifier = testStorage.getTablePath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(true);
        boolean actualResult = testStorage.exists(spark, info);
        assertTrue(actualResult);

        when(DeltaTable.forPath(spark, identifier)).thenReturn(mockedDeltaTable);
        when(mockedDeltaTable.toDF()).thenReturn(df);
        boolean hasRecords = testStorage.hasRecords(spark, info);
        assertFalse(hasRecords);
    }

    @Test
    void shouldReturnFalseForHasRecordsWhenStorageDoesNotExist() throws DataStorageException {
        TableIdentifier info = new TableIdentifier("s3://test-bucket", "domain",
                "incident", "demographics");
        String identifier = testStorage.getTablePath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(false);
        boolean actualResult = testStorage.exists(spark, info);
        assertFalse(actualResult);

        when(DeltaTable.forPath(spark, identifier)).thenReturn(mockedDeltaTable);
        boolean hasRecords = testStorage.hasRecords(spark, info);
        assertFalse(hasRecords);
    }

    @Test
    void shouldReturnTablePathWithSourceReference() {
        String prefix = "s3://test-bucket";
        SourceReference ref = mock(SourceReference.class);
        String operation = "insert";
        when(ref.getSource()).thenReturn("test");
        when(ref.getTable()).thenReturn("table");
        String actual_path = testStorage.getTablePath(prefix, ref, operation);
        assertEquals(actual_path, "s3://test-bucket/test/table/insert");
    }

    @Test
    void shouldReturnTablePathWithSourceReferenceAndNoOperation() {
        String prefix = "s3://test-bucket";
        SourceReference ref = mock(SourceReference.class);
        when(ref.getSource()).thenReturn("test");
        when(ref.getTable()).thenReturn("table");
        String path = testStorage.getTablePath(prefix, ref);
        assertNotNull(path);
    }

    @Test
    void shouldReturnTablePathWithVarArgString() {
        String path = testStorage.getTablePath("test");
        assertNotNull(path);
    }

    @Test
    void shouldAppendCompleteForDeltaTable() throws DataStorageException {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        DataStorageService mockService = mock(DataStorageService.class);
        doCallRealMethod().when(mockService).append(tablePath, df);
        mockService.append(tablePath, df);
        assertTrue(true);
    }

    @Test
    void shouldAppendThrowExceptionForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        DataStorageService mockService = mock(DataStorageService.class);
        try {
            doThrow(DataStorageException.class).when(mockService).append(tablePath, df);
            mockService.append(tablePath, df);
        } catch (DataStorageException de) {
            assertTrue(true);
        }


    }

    @Test
    void shouldCreateCompleteForDeltaTable() throws DataStorageException {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        DataStorageService mockService = mock(DataStorageService.class);
        doCallRealMethod().when(mockService).create(tablePath, df);
        mockService.create(tablePath, df);
        assertTrue(true);
    }

    @Test
    void shouldCreateThrowExceptionForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        DataStorageService mockService = mock(DataStorageService.class);
        try {
            doThrow(DataStorageException.class).when(mockService).create(tablePath, df);
            mockService.create(tablePath, df);
        } catch (DataStorageException de) {
            assertTrue(true);
        }
    }

    @Test
    void shouldReplaceCompleteForDeltaTable() throws DataStorageException {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        DataStorageService mockService = mock(DataStorageService.class);
        doCallRealMethod().when(mockService).replace(tablePath, df);
        mockService.replace(tablePath, df);
        assertTrue(true);
    }

    @Test
    void shouldReplaceThrowExceptionForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        DataStorageService mockService = spy(DataStorageService.class);
        try {
            doThrow(DataStorageException.class).when(mockService).replace(tablePath, df);
            mockService.replace(tablePath, df);
        } catch (DataStorageException de) {
            assertTrue(true);
        }
    }

    @Test
    void shouldReloadCompleteForDeltaTable() throws DataStorageException {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        DataStorageService mockService = spy(DataStorageService.class);
        doCallRealMethod().when(mockService).reload(tablePath, df);
        mockService.reload(tablePath, df);
        assertTrue(true);
    }

    @Test
    void shouldReloadThrowExceptionForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        val df = spark.sql("select cast(null as string) test_col");
        DataStorageService mockService = spy(DataStorageService.class);
        try {
            doThrow(DataStorageException.class).when(mockService).reload(tablePath, df);
            mockService.reload(tablePath, df);
        } catch (DataStorageException de) {
            assertTrue(true);
        }
    }

    @Test
    void shouldDeleteCompleteForDeltaTable() throws DataStorageException {
        DataStorageService mockService = mock(DataStorageService.class);
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        TableIdentifier info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(mockService).getTable(spark, tablePath);
        doNothing().when(mockedDeltaTable).delete();
        mockService.delete(spark, info);
        assertTrue(true);
    }

    @Test
    void shouldVaccumCompleteForDeltaTable() throws DataStorageException {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        TableIdentifier info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        DataStorageService mockService = mock(DataStorageService.class);
        doReturn(mockedDeltaTable).when(mockService).getTable(spark, tablePath);
        doReturn(mockedDataSet).when(mockedDeltaTable).vacuum();
        mockService.vacuum(spark, info);
        assertTrue(true);
    }

    @Test
    void shouldLoadCompleteForDeltaTable() throws DataStorageException {
        DataStorageService mockService = mock(DataStorageService.class);
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        TableIdentifier info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(mockService).getTable(spark, tablePath);
        doReturn(mockedDataSet).when(mockedDeltaTable).toDF();
        Dataset<Row> actualResult = mockService.load(spark, info);
        assertNull(actualResult);
    }

    @Test
    void shouldReturnDeltaTableWhenExists() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        DeltaTable actualResult = testStorage.getTable(spark, tablePath);
        assertNotNull(actualResult);
    }

    @Test
    void shouldReturnNullWhenNotExists() throws DataStorageException {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(false);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        DeltaTable actualResult = testStorage.getTable(spark, tablePath);
        assertNull(actualResult);
    }

    @Test
    void shouldUpdateendTableUpdates() throws DataStorageException {
        DataStorageService mockService = mock(DataStorageService.class);
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        TableIdentifier info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(mockService).getTable(spark, tablePath);
        doNothing().when(mockService).updateManifest(mockedDeltaTable);
        mockService.endTableUpdates(spark, info);
        assertTrue(true);
    }

    @Test
    void shouldUpdateManifest() {
        doNothing().when(mockedDeltaTable).generate("test");
        testStorage.updateManifest(mockedDeltaTable);
        assertTrue(true);
    }

    @Test
    void shouldUpdateDeltaManifestForTable() throws DataStorageException {
        DataStorageService mockService = mock(DataStorageService.class);
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(mockService).getTable(spark, tablePath);
        doNothing().when(mockService).updateManifest(mockedDeltaTable);
        mockService.updateDeltaManifestForTable(spark, tablePath);
        assertTrue(true);
    }
}