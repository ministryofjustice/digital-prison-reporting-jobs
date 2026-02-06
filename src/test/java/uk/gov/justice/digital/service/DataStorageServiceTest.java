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
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.datahub.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.config.JobArguments.APPROX_DATA_SIZE_GB_DEFAULT;
import static uk.gov.justice.digital.test.TestHelpers.givenConfiguredRetriesJobArgs;

@ExtendWith(MockitoExtension.class)
class DataStorageServiceTest extends SparkTestBase {

    private static final int DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES = 1;

    private static final SourceReference.PrimaryKey arbitraryPrimaryKey = new SourceReference.PrimaryKey("arbitrary");

    private MockedStatic<DeltaTable> mockDeltaTableStatic;

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private JobProperties mockJobProperties;
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
    private DataStorageService underTest;

    @TempDir
    private Path folder;

    private TableIdentifier tableId;

    private String tablePath;

    @BeforeEach
    void setUp() {
        givenRetrySettingsAreConfigured(mockJobArguments);
        givenParquetPartitionSettingsAreConfigured(mockJobArguments, mockJobProperties);

        mockDeltaTableStatic = mockStatic(DeltaTable.class);
        tableId = new TableIdentifier(
                folder.toAbsolutePath().toString(),
                "domain",
                "incident",
                "demographics"
        );
        tablePath = tableId.toPath();

        underTest = new DataStorageService(mockJobArguments, mockJobProperties);
    }

    @AfterEach
    void tearDown() {
        mockDeltaTableStatic.close();
    }

    @Test
    void shouldReturnTrueWhenStorageExists() {
        when(DeltaTable.isDeltaTable(spark, tablePath)).thenReturn(true);
        assertTrue(underTest.exists(spark, tableId));
    }

    @Test
    void shouldReturnFalseWhenStorageDoesNotExist() {
        when(DeltaTable.isDeltaTable(spark, tableId.toPath())).thenReturn(false);
        assertFalse(underTest.exists(spark, tableId));
    }

    @Test
    void shouldReturnTrueForHasRecordsWhenStorageExistsAndRecordsArePresent() {
        givenDeltaTableExists();

        val df = spark.sql("select cast(10 as LONG) as numFiles");

        when(mockDeltaTable.toDF()).thenReturn(df);

        assertTrue(underTest.exists(spark, tableId));
        assertTrue(underTest.hasRecords(spark, tableId));
    }

    @Test
    void shouldReturnFalseForHasRecordsWhenStorageExistsAndRecordsAreNotPresent() {
        givenDeltaTableExists();

        val df = spark.emptyDataFrame();

        when(mockDeltaTable.toDF()).thenReturn(df);

        assertTrue(underTest.exists(spark, tableId));
        assertFalse(underTest.hasRecords(spark, tableId));
    }

    @Test
    void shouldReturnFalseForHasRecordsWhenStorageDoesNotExist() {
        assertFalse(underTest.exists(spark, tableId));
        assertFalse(underTest.hasRecords(spark, tableId));
    }

    @Test
    void shouldAppendCompleteForDeltaTable() {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.mode(anyString())).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);

        underTest.append(tablePath, mockDataSet);

        verify(mockDataFrameWriter).mode("append");
        verify(mockDataFrameWriter).save();
    }

    @Test
    void shouldCreateCompleteForDeltaTable() {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);

        underTest.create(tablePath, mockDataSet);

        verify(mockDataFrameWriter).save();
    }

    @Test
    void shouldReplaceCompleteForDeltaTable() {
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
    void shouldReloadCompleteForDeltaTable() {
        when(mockDataSet.write()).thenReturn(mockDataFrameWriter);

        when(mockDataFrameWriter.format("delta")).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.mode(anyString())).thenReturn(mockDataFrameWriter);
        when(mockDataFrameWriter.option(anyString(), anyString())).thenReturn(mockDataFrameWriter);

        underTest.resync(tablePath, mockDataSet);

        verify(mockDataFrameWriter).mode("overwrite");
        verify(mockDataFrameWriter).save();
    }

    @Test
    void shouldDeleteCompleteForDeltaTable() {
        givenDeltaTableExists();
        underTest.delete(spark, tableId);
        verify(mockDeltaTable).delete();
    }

    @Test
    void shouldVacuumCompleteForDeltaTable() {
        givenDeltaTableExists();
        underTest.vacuum(spark, tableId);
        verify(mockDeltaTable).vacuum();
    }

    @Test
    void shouldVacuumForDeltaTablePath() {
        givenDeltaTableExists();
        underTest.vacuum(spark, tableId.toPath());
        verify(mockDeltaTable).vacuum();
    }

    @Test
    void shouldNotFailDuringVacuumWhenDeltaTableDoesNotExist() {
        assertDoesNotThrow(() -> underTest.vacuum(spark, tableId.toPath()));
    }

    @Test
    void shouldCompactDeltaTable() {
        givenDeltaTableExists();
        when(mockDeltaTable.optimize()).thenReturn(mockDeltaOptimize);

        underTest.compactDeltaTable(spark, tableId.toPath());
        verify(mockDeltaTable).optimize();
        verify(mockDeltaOptimize).executeCompaction();
    }

    @Test
    void shouldNotFailDuringCompactionWhenDeltaTableDoesNotExist() {
        assertDoesNotThrow(() -> underTest.compactDeltaTable(spark, tableId.toPath()));
    }

    @Test
    void shouldThrowForBadlyFormattedDeltaTablePath() {
        assertThrows(DataStorageException.class, () ->
                underTest.listDeltaTablePaths(spark, "://some-path", DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)
        );
    }

    @Test
    void shouldThrowWhenDeltaTablePathDoesNotExist() {
        assertThrows(DataStorageException.class, () ->
            underTest.listDeltaTablePaths(spark, "/doesnotexist", DEPTH_LIMIT_TO_RECURSE_DELTA_TABLES)
        );
    }

    @Test
    void shouldGetDeltaTableWhenExists() {
        givenDeltaTableExists();
        underTest.get(spark, tableId);
        verify(mockDeltaTable).toDF();
    }

    @Test
    void shouldGenerateManifestWhenEndTableUpdatesCalled() {
        givenDeltaTableExists();
        underTest.endTableUpdates(spark, tableId);
        verifyManifestGeneratedWithExpectedModeString();
    }

    @Test
    void shouldUpdateDeltaManifestForTable() {
        givenDeltaTableExists();
        underTest.updateDeltaManifestForTable(spark, tablePath);
        verifyManifestGeneratedWithExpectedModeString();
    }

    @Test
    void shouldRetryAppendAndSucceedEventually() {
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenSaveThrowsFirstTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        dataStorageService.append(tablePath, mockDataSet);
        verify(mockDataFrameWriter, times(2)).save();
    }

    @Test
    void shouldRetryAppendDistinctAndSucceedEventually() {
        stubAppendDistinct();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenMergeThrowsFirstTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        dataStorageService.appendDistinct(tablePath, mockDataSet, new SourceReference.PrimaryKey("arbitrary"));
        verify(mockDeltaMergeBuilder, times(2)).execute();
    }

    @Test
    void shouldRetryUpdateRecordsAndSucceedEventually() {
        stubUpdateRecords();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenMergeThrowsFirstTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        dataStorageService.updateRecords(spark, tablePath, mockDataSet, new SourceReference.PrimaryKey("arbitrary"));
        verify(mockDeltaMergeBuilder, times(2)).execute();
    }

    @Test
    void shouldRetryMergeRecordsAndSucceedEventually() {
        when(mockDataSet.columns()).thenReturn(new String[0]);

        stubDeltaTableCreateIfNotExists();
        stubMergeRecordsCdc();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenMergeThrowsFirstTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        dataStorageService.mergeRecords(spark, tablePath, mockDataSet, new SourceReference.PrimaryKey("arbitrary"));
        verify(mockDeltaMergeBuilder, times(2)).execute();
    }

    @Test
    void shouldRetryCompactAndSucceedEventually() {
        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenCompactThrowsFirstTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        dataStorageService.compactDeltaTable(spark, tablePath);
        verify(mockDeltaOptimize, times(2)).executeCompaction();
    }

    @Test
    void shouldRetryVacuumAndSucceedEventually() {
        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(3, mockJobArguments);
        givenVacuumThrowsFirstTime(ConcurrentDeleteReadException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        dataStorageService.vacuum(spark, tablePath);
        verify(mockDeltaTable, times(2)).vacuum();
    }

    @Test
    void shouldRetryAppendAndFailEventually() {
        int retryAttempts = 5;

        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenSaveThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> dataStorageService.append(tablePath, mockDataSet));
        verify(mockDataFrameWriter, times(retryAttempts)).save();
    }
    @Test
    void shouldRetryAppendDistinctAndFailEventually() {
        int retryAttempts = 5;

        stubAppendDistinct();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenMergeThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> {
            dataStorageService.appendDistinct(tablePath, mockDataSet, arbitraryPrimaryKey);
        });
        verify(mockDeltaMergeBuilder, times(retryAttempts)).execute();
    }

    @Test
    void shouldRetryMergeRecordsAndFailEventually() {
        int retryAttempts = 5;
        when(mockDataSet.columns()).thenReturn(new String[0]);

        stubDeltaTableCreateIfNotExists();
        stubMergeRecordsCdc();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenMergeThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> {
            dataStorageService.mergeRecords(spark, tablePath, mockDataSet, arbitraryPrimaryKey);
        });
        verify(mockDeltaMergeBuilder, times(retryAttempts)).execute();
    }

    @Test
    void shouldRetryUpdateRecordsAndFailEventually() {
        int retryAttempts = 5;

        stubUpdateRecords();

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenMergeThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> {
            dataStorageService.updateRecords(spark, tablePath, mockDataSet, arbitraryPrimaryKey);
        });
        verify(mockDeltaMergeBuilder, times(retryAttempts)).execute();
    }

    @Test
    void shouldRetryCompactAndFailEventually() {
        int retryAttempts = 5;

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenCompactThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> {
            dataStorageService.compactDeltaTable(spark, tablePath);
        });
        verify(mockDeltaOptimize, times(retryAttempts)).executeCompaction();
    }

    @Test
    void shouldRetryVacuumAndFailEventually() {
        int retryAttempts = 5;

        givenDeltaTableExists();
        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        givenVacuumThrowsEveryTime(ConcurrentAppendException.class);

        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);
        assertThrows(DataStorageRetriesExhaustedException.class, () -> {
            dataStorageService.vacuum(spark, tablePath);
        });
        verify(mockDeltaTable, times(retryAttempts)).vacuum();
    }

    @Test
    void shouldOverwriteParquet() {
        int retryAttempts = 1;
        double approxDataSize = 3.0;
        int sparkExecutorCores = 2;
        // - APPROX_DATA_SIZE_GB = 3.0 GB
        // - Max partition target size: 128 MB
        // - NUM_EXECUTOR_CORES = 2
        // Min number of 128 MB partitions:
        // (2 * 3 * 1024 MB) / 128 MB = 48 partitions
        int numPartitions = 48;

        givenConfiguredRetriesJobArgs(retryAttempts, mockJobArguments);
        when(mockJobArguments.getApproxDataSizeGigaBytes()).thenReturn(approxDataSize);
        when(mockJobProperties.getSparkExecutorCores()).thenReturn(sparkExecutorCores);
        DataStorageService dataStorageService = new DataStorageService(mockJobArguments, mockJobProperties);

        when(mockDataSet.coalesce(numPartitions)).thenReturn(mockDataSet);
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
