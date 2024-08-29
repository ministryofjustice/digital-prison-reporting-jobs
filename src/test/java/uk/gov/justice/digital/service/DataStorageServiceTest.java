package uk.gov.justice.digital.service;

import io.delta.exceptions.ConcurrentAppendException;
import io.delta.exceptions.ConcurrentDeleteReadException;
import io.delta.tables.DeltaMergeBuilder;
import io.delta.tables.DeltaMergeMatchedActionBuilder;
import io.delta.tables.DeltaMergeNotMatchedActionBuilder;
import io.delta.tables.DeltaOptimizeBuilder;
import io.delta.tables.DeltaTable;
import io.delta.tables.DeltaTableBuilder;
import lombok.val;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.datahub.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;

import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataStorageServiceTest extends BaseSparkTest {

    private static final int DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES = 1;

    private static final DataStorageService underTest = new DataStorageService(new JobArguments(Collections.emptyMap()));


    private MockedStatic<DeltaTable> mockDeltaTableStatic;

    @Mock
    private DeltaTable mockDeltaTable;
    @Mock
    private DeltaTableBuilder mockDeltaTableBuilder;

    @Mock
    private DeltaOptimizeBuilder mockDeltaOptimize;

    @Mock
    private Dataset<Row> mockDataSet;

    @Mock
    private DataFrameWriter<Row> mockDataFrameWriter;

    @Mock
    private DeltaMergeBuilder mockDeltaMergeBuilder;
    @Mock
    private DeltaMergeNotMatchedActionBuilder mockDeltaMergeNotMatchedActionBuilder;
    @Mock
    private DeltaMergeMatchedActionBuilder mockDeltaMergeMatchedActionBuilder;

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
    public void shouldReturnTrueForHasRecordsWhenStorageExistsAndRecordsArePresent() {
        givenDeltaTableExists();

        val df = spark.sql("select cast(10 as LONG) as numFiles");

        when(mockDeltaTable.toDF()).thenReturn(df);

        assertTrue(underTest.exists(spark, tableId));
        assertTrue(underTest.hasRecords(spark, tableId));
    }

    @Test
    public void shouldReturnFalseForHasRecordsWhenStorageExistsAndRecordsAreNotPresent() {
        givenDeltaTableExists();

        val df = spark.emptyDataFrame();

        when(mockDeltaTable.toDF()).thenReturn(df);

        assertTrue(underTest.exists(spark, tableId));
        assertFalse(underTest.hasRecords(spark, tableId));
    }

    @Test
    public void shouldReturnFalseForHasRecordsWhenStorageDoesNotExist() {
        assertFalse(underTest.exists(spark, tableId));
        assertFalse(underTest.hasRecords(spark, tableId));
    }

    @Test
    public void shouldAppendCompleteForDeltaTable() {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.mode(anyString())).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);

        underTest.append(tablePath, mockDataSet);

        verify(mockDataFrameWriter).mode("append");
        verify(mockDataFrameWriter).save();
    }

    @Test
    public void shouldCreateCompleteForDeltaTable() {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);

        underTest.create(tablePath, mockDataSet);

        verify(mockDataFrameWriter).save();
    }

    @Test
    public void shouldReplaceCompleteForDeltaTable() {
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
    public void shouldReloadCompleteForDeltaTable() {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.mode(anyString())).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);

        underTest.resync(tablePath, mockDataSet);

        verify(mockDataFrameWriter).mode("overwrite");
        verify(mockDataFrameWriter).save();
    }

    @Test
    public void shouldDeleteCompleteForDeltaTable() {
        givenDeltaTableExists();
        underTest.delete(spark, tableId);
        verify(mockDeltaTable).delete();
    }

    @Test
    public void shouldVacuumCompleteForDeltaTable() {
        givenDeltaTableExists();
        underTest.vacuum(spark, tableId);
        verify(mockDeltaTable).vacuum();
    }

    @Test
    public void shouldVacuumForDeltaTablePath() {
        givenDeltaTableExists();
        underTest.vacuum(spark, tableId.toPath());
        verify(mockDeltaTable).vacuum();
    }

    @Test
    public void shouldCompactDeltaTable() {
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
    public void shouldGenerateManifestWhenEndTableUpdatesCalled() {
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

    @Test
    public void shouldRetryAppendAndSucceedEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);

        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenSaveThrowsFirstTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        dataStorageService.append(tablePath, mockDataSet);
        verify(mockDataFrameWriter, times(2)).save();
    }

    @Test
    public void shouldRetryAppendDistinctAndSucceedEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);

        stubAppendDistinct();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenMergeThrowsFirstTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        dataStorageService.appendDistinct(tablePath, mockDataSet, new SourceReference.PrimaryKey("arbitrary"));
        verify(mockDeltaMergeBuilder, times(2)).execute();
    }

    @Test
    public void shouldRetryUpdateRecordsAndSucceedEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);

        stubUpdateRecords();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenMergeThrowsFirstTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        dataStorageService.updateRecords(spark, tablePath, mockDataSet, new SourceReference.PrimaryKey("arbitrary"));
        verify(mockDeltaMergeBuilder, times(2)).execute();
    }

    @Test
    public void shouldRetryMergeRecordsAndSucceedEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);
        when(mockDataSet.columns()).thenReturn(new String[0]);

        stubDeltaTableCreateIfNotExists();
        stubMergeRecordsCdc();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenMergeThrowsFirstTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        dataStorageService.mergeRecords(spark, tablePath, mockDataSet, new SourceReference.PrimaryKey("arbitrary"));
        verify(mockDeltaMergeBuilder, times(2)).execute();
    }

    @Test
    public void shouldRetryCompactAndSucceedEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenCompactThrowsFirstTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        dataStorageService.compactDeltaTable(spark, tablePath);
        verify(mockDeltaOptimize, times(2)).executeCompaction();
    }

    @Test
    public void shouldRetryVacuumAndSucceedEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenVacuumThrowsFirstTime(ConcurrentDeleteReadException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        dataStorageService.vacuum(spark, tablePath);
        verify(mockDeltaTable, times(2)).vacuum();
    }

    @Test
    public void shouldRetryAppendAndFailEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);
        int retryAttempts = 5;

        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenSaveThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> dataStorageService.append(tablePath, mockDataSet));
        verify(mockDataFrameWriter, times(retryAttempts)).save();
    }
    @Test
    public void shouldRetryAppendDistinctAndFailEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);
        int retryAttempts = 5;

        stubAppendDistinct();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenMergeThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> {
            dataStorageService.appendDistinct(tablePath, mockDataSet, new SourceReference.PrimaryKey("arbitrary"));
        });
        verify(mockDeltaMergeBuilder, times(retryAttempts)).execute();
    }

    @Test
    public void shouldRetryMergeRecordsAndFailEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);
        int retryAttempts = 5;
        when(mockDataSet.columns()).thenReturn(new String[0]);

        stubDeltaTableCreateIfNotExists();
        stubMergeRecordsCdc();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenMergeThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> {
            dataStorageService.mergeRecords(spark, tablePath, mockDataSet, new SourceReference.PrimaryKey("arbitrary"));
        });
        verify(mockDeltaMergeBuilder, times(retryAttempts)).execute();
    }

    @Test
    public void shouldRetryUpdateRecordsAndFailEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);
        int retryAttempts = 5;

        stubUpdateRecords();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenMergeThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> {
            dataStorageService.updateRecords(spark, tablePath, mockDataSet, new SourceReference.PrimaryKey("arbitrary"));
        });
        verify(mockDeltaMergeBuilder, times(retryAttempts)).execute();
    }

    @Test
    public void shouldRetryCompactAndFailEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);
        int retryAttempts = 5;

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenCompactThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> {
            dataStorageService.compactDeltaTable(spark, tablePath);
        });
        verify(mockDeltaOptimize, times(retryAttempts)).executeCompaction();
    }

    @Test
    public void shouldRetryVacuumAndFailEventually() {
        JobArguments mockJobArguments = mock(JobArguments.class);
        int retryAttempts = 5;

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenVacuumThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> {
            dataStorageService.vacuum(spark, tablePath);
        });
        verify(mockDeltaTable, times(retryAttempts)).vacuum();
    }

    @Test
    void shouldOverwriteParquet() {
        JobArguments mockJobArguments = mock(JobArguments.class);
        int retryAttempts = 1;
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        DataStorageService dataStorageService = new DataStorageService(mockJobArguments);

        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.mode(SaveMode.Overwrite)).thenReturn(mockDataFrameWriter);
        String path = "some-path";

        dataStorageService.overwriteParquet(path, mockDataSet);

        verify(mockDataFrameWriter, times(1)).parquet(path);
    }

    private void givenSaveThrowsEveryTime(Class<? extends Throwable> toBeThrown) {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.mode(anyString())).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);
        doThrow(toBeThrown).when(mockDataFrameWriter).save();
    }

    private void givenSaveThrowsFirstTime(Class<? extends Throwable> toBeThrown) {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.mode(anyString())).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);
        doThrow(toBeThrown)
                .doNothing()
                .when(mockDataFrameWriter).save();
    }

    private void givenConfiguredRetriesJobArgs(int numRetries, JobArguments mockJobArguments) {
        when(mockJobArguments.getDataStorageRetryMaxAttempts()).thenReturn(numRetries);
        when(mockJobArguments.getDataStorageRetryMinWaitMillis()).thenReturn(1L);
        when(mockJobArguments.getDataStorageRetryMaxWaitMillis()).thenReturn(10L);
        when(mockJobArguments.getDataStorageRetryJitterFactor()).thenReturn(0.1D);
    }

    private void givenMergeThrowsEveryTime(Class<? extends Throwable> toBeThrown) {
        doThrow(toBeThrown).when(mockDeltaMergeBuilder).execute();
    }

    private void givenMergeThrowsFirstTime(Class<? extends Throwable> toBeThrown) {
        doThrow(toBeThrown)
                .doNothing()
                .when(mockDeltaMergeBuilder).execute();
    }

    private void givenCompactThrowsEveryTime(Class<? extends Throwable> toBeThrown) {
        when(mockDeltaTable.optimize()).thenReturn(mockDeltaOptimize);
        when(mockDeltaOptimize.executeCompaction()).thenThrow(toBeThrown);
    }

    private void givenCompactThrowsFirstTime(Class<? extends Throwable> toBeThrown) {
        when(mockDeltaTable.optimize()).thenReturn(mockDeltaOptimize);
        when(mockDeltaOptimize.executeCompaction())
                .thenThrow(toBeThrown)
                .thenReturn(spark.emptyDataFrame());
    }

    private void givenVacuumThrowsEveryTime(Class<? extends Throwable> toBeThrown) {
        when(mockDeltaTable.vacuum()).thenThrow(toBeThrown);
    }

    private void givenVacuumThrowsFirstTime(Class<? extends Throwable> toBeThrown) {
        when(mockDeltaTable.vacuum())
                .thenThrow(toBeThrown)
                .thenReturn(spark.emptyDataFrame());
    }

    private void stubAppendDistinct() {
        when(mockDeltaTable.as(anyString())).thenReturn(mockDeltaTable);
        when(mockDeltaTable.merge(any(), anyString())).thenReturn(mockDeltaMergeBuilder);
        when(mockDeltaMergeBuilder.whenNotMatched()).thenReturn(mockDeltaMergeNotMatchedActionBuilder);
        when(mockDeltaMergeNotMatchedActionBuilder.insertAll()).thenReturn(mockDeltaMergeBuilder);
    }

    private void stubMergeRecordsCdc() {
        when(mockDeltaTable.as(anyString())).thenReturn(mockDeltaTable);
        when(mockDeltaTable.merge(any(), anyString())).thenReturn(mockDeltaMergeBuilder);
        when(mockDeltaMergeBuilder.whenMatched(anyString())).thenReturn(mockDeltaMergeMatchedActionBuilder);
        when(mockDeltaMergeMatchedActionBuilder.updateExpr(anyMap())).thenReturn(mockDeltaMergeBuilder);
        when(mockDeltaMergeBuilder.whenMatched(anyString())).thenReturn(mockDeltaMergeMatchedActionBuilder);
        when(mockDeltaMergeMatchedActionBuilder.delete()).thenReturn(mockDeltaMergeBuilder);
        when(mockDeltaMergeBuilder.whenNotMatched(anyString())).thenReturn(mockDeltaMergeNotMatchedActionBuilder);
        when(mockDeltaMergeNotMatchedActionBuilder.insertExpr(anyMap())).thenReturn(mockDeltaMergeBuilder);
    }

    private void stubDeltaTableCreateIfNotExists() {
        when(DeltaTable.createIfNotExists(any())).thenReturn(mockDeltaTableBuilder);
        when(mockDeltaTableBuilder.addColumns(any())).thenReturn(mockDeltaTableBuilder);
        when(mockDeltaTableBuilder.location(any())).thenReturn(mockDeltaTableBuilder);
        when(mockDeltaTableBuilder.execute()).thenReturn(mockDeltaTable);
    }

    private void stubUpdateRecords() {
        when(mockDeltaTable.as(anyString())).thenReturn(mockDeltaTable);
        when(mockDeltaTable.merge(any(), anyString())).thenReturn(mockDeltaMergeBuilder);
        when(mockDeltaMergeBuilder.whenMatched()).thenReturn(mockDeltaMergeMatchedActionBuilder);
        when(mockDeltaMergeMatchedActionBuilder.updateAll()).thenReturn(mockDeltaMergeBuilder);
    }

    private void givenDeltaTableExists() {
        when(DeltaTable.isDeltaTable(any(), eq(tablePath))).thenReturn(true);
        when(DeltaTable.forPath(any(), eq(tablePath))).thenReturn(mockDeltaTable);
    }

    private void verifyManifestGeneratedWithExpectedModeString() {
        verify(mockDeltaTable).generate("symlink_format_manifest");
    }
}
