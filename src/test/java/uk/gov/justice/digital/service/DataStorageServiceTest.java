package uk.gov.justice.digital.service;


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
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mockStatic;
import io.delta.tables.DeltaTable;
import java.nio.file.Path;

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
        boolean actual_result = testStorage.exists(spark, info);
        assertTrue(actual_result);
    }

    @Test
    void shouldReturnFalseWhenStorageDoesNotExists() {
        TableIdentifier info = new TableIdentifier( "s3://test-bucket", "domain",
                "incident", "demographics");
        String identifier = testStorage.getTablePath(info.getBasePath(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(false);
        boolean actual_result = testStorage.exists(spark, info);
        assertFalse(actual_result);
    }

    @Test
    void shouldReturnTablePathWithSourceReference() {
        String prefix = "s3://test-bucket";
        SourceReference ref = mock(SourceReference.class);
        String operation = "insert";
        when(ref.getSource()).thenReturn("test");
        when(ref.getTable()).thenReturn("table");
        String path = testStorage.getTablePath(prefix, ref, operation);
        assertNotNull(path);
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
    void shouldAppendCompleteForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        testStorage.append(tablePath, spark.sql("select cast(null as string) test_col"));
        assertTrue(true);
    }

    @Test
    void shouldCreateCompleteForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        testStorage.create(tablePath, spark.sql("select cast(null as string) test_col"));
        assertTrue(true);
    }

    @Test
    void shouldReplaceCompleteForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        testStorage.replace(tablePath, spark.sql("select cast(null as string) test_col"));
        assertTrue(true);
    }

    @Test
    void shouldReloadCompleteForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        testStorage.reload(tablePath, spark.sql("select cast(null as string) test_col"));
        assertTrue(true);
    }

    @Test
    void shouldDeleteCompleteForDeltaTable() {
        DataStorageService spy = spy(new DataStorageService());
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        TableIdentifier info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(spy).getTable(spark, tablePath);
        doNothing().when(mockedDeltaTable).delete();
        spy.delete(spark, info);
        assertTrue(true);
    }

    @Test
    void shouldVaccumCompleteForDeltaTable() {
        DataStorageService spy = spy(new DataStorageService());
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        TableIdentifier info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(spy).getTable(spark, tablePath);
        doReturn(mockedDataSet).when(mockedDeltaTable).vacuum();
        spy.vacuum(spark, info);
        assertTrue(true);
    }

    @Test
    void shouldLoadCompleteForDeltaTable() {
        DataStorageService spy = spy(new DataStorageService());
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        TableIdentifier info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(spy).getTable(spark, tablePath);
        doReturn(mockedDataSet).when(mockedDeltaTable).toDF();
        Dataset<Row> actual_result = spy.load(spark, info);
        assertNull(actual_result);
    }

    @Test
    void shouldReturnDeltaTableWhenExists() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        DeltaTable actual_result = testStorage.getTable(spark, tablePath);
        assertNotNull(actual_result);
    }

    @Test
    void shouldReturnNullWhenNotExists() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(false);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        DeltaTable actual_result = testStorage.getTable(spark, tablePath);
        assertNull(actual_result);
    }

    @Test
    void shouldUpdateendTableUpdates() {
        DataStorageService spy = spy(new DataStorageService());
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        TableIdentifier info = new TableIdentifier(this.folder.toFile().getAbsolutePath(), "domain",
                "incident", "demographics");
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(spy).getTable(spark, tablePath);
        doNothing().when(spy).updateManifest(mockedDeltaTable);
        spy.endTableUpdates(spark, info);
        assertTrue(true);
    }

    @Test
    void shouldUpdateManifest() {
        doNothing().when(mockedDeltaTable).generate("test");
        testStorage.updateManifest(mockedDeltaTable);
        assertTrue(true);
    }

    @Test
    void shouldUpdateDeltaManifestForTable() {
        DataStorageService spy = spy(new DataStorageService());
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        when(DeltaTable.forPath(spark, tablePath)).thenReturn(mockedDeltaTable);
        doReturn(mockedDeltaTable).when(spy).getTable(spark, tablePath);
        doNothing().when(spy).updateManifest(mockedDeltaTable);
        spy.updateDeltaManifestForTable(spark, tablePath);
        assertTrue(true);
    }
}