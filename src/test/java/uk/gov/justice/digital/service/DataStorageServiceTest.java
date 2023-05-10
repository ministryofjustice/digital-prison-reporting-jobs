package uk.gov.justice.digital.service;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.domain.model.TableInfo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mockStatic;
import io.delta.tables.DeltaTable;
import java.nio.file.Path;

class DataStorageServiceTest extends BaseSparkTest {

    private static DataStorageService testStorage;
    private MockedStatic<DeltaTable> mockedStatic;

    @TempDir
    private Path folder;

    @BeforeEach
    void setUp() {
        testStorage = new DataStorageService();
        mockedStatic = mockStatic(DeltaTable.class);
    }

    @AfterEach
    void tearDown() {
        mockedStatic.close();
    }

    @Test
    void shouldReturnTrueWhenStorageExists() {
        TableInfo info = new TableInfo();
        info.setDatabase("domain");
        info.setPrefix("s3://test-bucket");
        info.setSchema("incident");
        info.setTable("demographics");
        String identifier = testStorage.getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
        when(DeltaTable.isDeltaTable(spark, identifier)).thenReturn(true);
        boolean actual_result = testStorage.exists(spark, info);
        assertTrue(actual_result);
    }

    @Test
    void shouldReturnFalseWhenStorageDoesNotExists() {
        TableInfo info = new TableInfo();
        info.setDatabase("domain");
        info.setPrefix("s3://test-bucket");
        info.setSchema("incident");
        info.setTable("demographics");
        String identifier = testStorage.getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
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
        String prefix = "s3://test-bucket";
        String path = testStorage.getTablePath("test");
        assertNotNull(path);
    }

    @Test
    void shouldAppendCompleteForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        testStorage.append(tablePath, spark.sql("select cast(null as string) test_col"));
    }

    @Test
    void shouldCreateCompleteForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        testStorage.create(tablePath, spark.sql("select cast(null as string) test_col"));
    }

    @Test
    void shouldReplaceCompleteForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        testStorage.replace(tablePath, spark.sql("select cast(null as string) test_col"));
    }

    @Test
    void shouldReloadCompleteForDeltaTable() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        testStorage.reload(tablePath, spark.sql("select cast(null as string) test_col"));
    }

    @Test
    void shouldDeleteCompleteForDeltaTable() {
        TableInfo info = new TableInfo();
        info.setPrefix(this.folder.toFile().getAbsolutePath());
        info.setSchema("incident");
        info.setTable("demographics");
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
//        when(testStorage.getTable(any(), any())).thenReturn();
        testStorage.delete(spark, info);
    }

    @Test
    void shouldVaccumCompleteForDeltaTable() {
        TableInfo info = new TableInfo();
        info.setPrefix("s3://test-bucket");
        info.setSchema("incident");
        info.setTable("demographics");
        testStorage.vacuum(spark, info);
    }

    @Test
    void shouldLoadCompleteForDeltaTable() {
        TableInfo info = new TableInfo();
        info.setPrefix("s3://test-bucket");
        info.setSchema("incident");
        info.setTable("demographics");
        testStorage.load(spark, info);
    }

    @Test
    void shouldReturnDeltaTableWhenExists() {
        final String tablePath = this.folder.toFile().getAbsolutePath() + "/source";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        DeltaTable actual_result = testStorage.getTable(spark, tablePath);
        System.out.println(actual_result);
        assertNotNull(actual_result);
    }

    @Test
    void shouldReturnNullWhenNotExists() {
        String tablePath = "s3://test-bucket";
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(false);
        DeltaTable actual_result = testStorage.getTable(spark, tablePath);
        assertNull(actual_result);
    }

    @Test
    void endTableUpdates() {
    }

    @Test
    void updateManifest() {
    }

    @Test
    void updateDeltaManifestForTable() {
    }
}