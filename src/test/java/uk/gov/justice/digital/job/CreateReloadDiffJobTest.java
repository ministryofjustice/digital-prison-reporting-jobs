package uk.gov.justice.digital.job;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.SchemaNotFoundException;
import uk.gov.justice.digital.exception.TableDiscoveryException;
import uk.gov.justice.digital.job.batchprocessing.ReloadDiffProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DmsOrchestrationService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.CHECKPOINT_COL_VALUE;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.SparkTestHelpers.containsTheSameElementsInOrderAs;

@ExtendWith(MockitoExtension.class)
class CreateReloadDiffJobTest extends BaseSparkTest {
    private static final String DMS_TASK_ID = "some-dms-task-id";
    private static final String RAW_PATH = "s3://raw-bucket/";
    private static final String ARCHIVE_PATH = "s3://raw-archive-bucket/";
    private static final String TEMP_RELOAD_PATH = "s3://temp-reload-bucket/";
    private static final String OUTPUT_FOLDER = "temp-reload-output";
    private static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();

    private static final Map<ImmutablePair<String, String>, List<String>> discoveredRawPathsByTable;

    private static final ImmutablePair<String, String> s1T1 = ImmutablePair.of("s1", "t1");
    private static final ImmutablePair<String, String> s2T2 = ImmutablePair.of("s2", "t2");
    private static final ImmutablePair<String, String> s3T3 = ImmutablePair.of("s3", "t3");
    private static final ImmutablePair<String, String> s4T4 = ImmutablePair.of("s4", "t4");

    static {
        discoveredRawPathsByTable = new HashMap<>();
        discoveredRawPathsByTable.put(s1T1, Collections.singletonList("raw-t1-file1"));
        discoveredRawPathsByTable.put(s2T2, Arrays.asList("raw-t2-file1", "raw-t2-file2"));
        discoveredRawPathsByTable.put(s3T3, Collections.singletonList("raw-t3-file1"));
        discoveredRawPathsByTable.put(s4T4, Collections.emptyList());
    }

    private static final Map<ImmutablePair<String, String>, List<String>> discoveredArchivePathsByTable;

    static {
        discoveredArchivePathsByTable = new HashMap<>();
        discoveredArchivePathsByTable.put(s1T1, Collections.singletonList("archive-t1-file1"));
        discoveredArchivePathsByTable.put(s2T2, Collections.singletonList("archive-t2-file1"));
    }

    @Mock
    private JobArguments arguments;
    @Mock
    private JobProperties properties;
    @Mock
    private S3DataProvider dataProvider;
    @Mock
    private TableDiscoveryService tableDiscoveryService;
    @Mock
    private DmsOrchestrationService dmsOrchestrationService;
    @Mock
    private ReloadDiffProcessor reloadDiffProcessor;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Captor
    private ArgumentCaptor<String> outputPathCaptor;
    @Captor
    private ArgumentCaptor<SourceReference> sourceRefCaptor;
    @Captor
    private ArgumentCaptor<Dataset<Row>> rawDatasetCaptor;
    @Captor
    private ArgumentCaptor<Dataset<Row>> archiveDatasetCaptor;
    private CreateReloadDiffJob underTest;

    @BeforeEach
    public void setUp() {
        Mockito.reset(
                arguments,
                properties,
                dataProvider,
                tableDiscoveryService,
                dmsOrchestrationService,
                reloadDiffProcessor,
                sourceReferenceService
        );

        underTest = new CreateReloadDiffJob(
                arguments,
                properties,
                dataProvider,
                sparkSessionProvider,
                tableDiscoveryService,
                dmsOrchestrationService,
                reloadDiffProcessor,
                sourceReferenceService
        );
    }

    @Test
    void shouldCreateReloadDiffForDiscoveredTables() {
        Date dmsTaskStartTime = Date.from(Instant.now());

        when(arguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(arguments.getRawS3Path()).thenReturn(RAW_PATH);
        when(arguments.getRawArchiveS3Path()).thenReturn(ARCHIVE_PATH);
        when(arguments.getTempReloadS3Path()).thenReturn(TEMP_RELOAD_PATH);
        when(arguments.getTempReloadOutputFolder()).thenReturn(OUTPUT_FOLDER);
        when(dmsOrchestrationService.getTaskStartTime(DMS_TASK_ID)).thenReturn(dmsTaskStartTime);
        when(tableDiscoveryService.discoverBatchFilesToLoad(RAW_PATH, spark)).thenReturn(discoveredRawPathsByTable);
        when(tableDiscoveryService.discoverBatchFilesToLoad(ARCHIVE_PATH, spark)).thenReturn(discoveredArchivePathsByTable);
        Stream.of(s2T2, s1T1, s3T3).forEach(this::mockSourceReference);

        Dataset<Row> rawDataset1 = spark.createDataFrame(Collections.singletonList(
                        createRow(1, "2023-11-13 10:50:00.123456", Insert, "rawRecord_1_1")),
                TEST_DATA_SCHEMA
        );
        Dataset<Row> rawDataset2 = spark.createDataFrame(Arrays.asList(
                        createRow(2, "2023-11-13 10:50:00.123456", Insert, "rawRecord_2_1"),
                        createRow(3, "2023-11-13 10:50:00.123456", Insert, "rawRecord_3_1")
                ),
                TEST_DATA_SCHEMA
        );
        Dataset<Row> rawDataset3 = spark.createDataFrame(Arrays.asList(
                        createRow(4, "2023-11-13 10:50:00.123456", Insert, "rawRecord_4_1"),
                        createRow(5, "2023-11-13 10:50:00.123456", Insert, "rawRecord_5_1")
                ),
                TEST_DATA_SCHEMA
        );

        Dataset<Row> archiveDataset1 = spark.createDataFrame(Collections.singletonList(
                        createRow(5, "2023-11-13 10:50:00.123456", Insert, "archiveRecord_4_1")),
                TEST_DATA_SCHEMA
        );
        Dataset<Row> archiveDataset2 = spark.createDataFrame(Arrays.asList(
                        RowFactory.create(6, "2023-11-13 10:50:00.123456", Insert.getName(), "archiveRecord_5_1", CHECKPOINT_COL_VALUE),
                        RowFactory.create(7, "2023-11-13 10:50:00.123456", Insert.getName(), "archiveRecord_6_1", CHECKPOINT_COL_VALUE)
                ),
                TEST_DATA_SCHEMA
        );

        when(dataProvider.getBatchSourceData(spark, Collections.singletonList("raw-t1-file1"))).thenReturn(rawDataset1);
        when(dataProvider.getBatchSourceData(spark, Arrays.asList("raw-t2-file1", "raw-t2-file2"))).thenReturn(rawDataset2);
        when(dataProvider.getBatchSourceData(spark, Collections.singletonList("raw-t3-file1"))).thenReturn(rawDataset3);
        when(dataProvider.getBatchSourceData(spark, Collections.singletonList("archive-t1-file1"))).thenReturn(archiveDataset1);
        when(dataProvider.getBatchSourceData(spark, Collections.singletonList("archive-t2-file1"))).thenReturn(archiveDataset2);

        underTest.runJob(spark);

        verify(reloadDiffProcessor, times(3)).createDiff(
                sourceRefCaptor.capture(),
                outputPathCaptor.capture(),
                rawDatasetCaptor.capture(),
                archiveDatasetCaptor.capture(),
                eq(dmsTaskStartTime)
        );

        List<String> expectedOutputPaths = Arrays.asList(
                TEMP_RELOAD_PATH + OUTPUT_FOLDER,
                TEMP_RELOAD_PATH + OUTPUT_FOLDER,
                TEMP_RELOAD_PATH + OUTPUT_FOLDER
        );

        List<SourceReference> expectedSourceRefs = Stream.of(s2T2, s1T1, s3T3)
                .map(CreateReloadDiffJobTest::createSourceReference)
                .collect(Collectors.toList());

        assertThat(outputPathCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedOutputPaths));
        assertThat(sourceRefCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedSourceRefs));
        assertThat(
                collectRecords(rawDatasetCaptor.getAllValues()),
                containsTheSameElementsInOrderAs(collectRecords(Arrays.asList(rawDataset2, rawDataset1, rawDataset3)))
        );
        assertThat(
                collectRecords(archiveDatasetCaptor.getAllValues()),
                containsTheSameElementsInOrderAs(collectRecords(Arrays.asList(archiveDataset2, archiveDataset1)))
        );
    }

    @Test
    void shouldCreateDiffUsingAnEmptyDatasetWhenNoArchiveFileExists() {
        Date dmsTaskStartTime = new Date();

        when(arguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(arguments.getRawS3Path()).thenReturn(RAW_PATH);
        when(arguments.getRawArchiveS3Path()).thenReturn(ARCHIVE_PATH);
        when(arguments.getTempReloadS3Path()).thenReturn(TEMP_RELOAD_PATH);
        when(arguments.getTempReloadOutputFolder()).thenReturn(OUTPUT_FOLDER);
        when(dmsOrchestrationService.getTaskStartTime(DMS_TASK_ID)).thenReturn(dmsTaskStartTime);
        when(tableDiscoveryService.discoverBatchFilesToLoad(RAW_PATH, spark))
                .thenReturn(Collections.singletonMap(s1T1, Collections.singletonList("raw-t1-file1")));
        when(tableDiscoveryService.discoverBatchFilesToLoad(ARCHIVE_PATH, spark)).thenReturn(Collections.emptyMap());
        mockSourceReference(s1T1);

        Dataset<Row> dataset = spark.createDataFrame(Collections.singletonList(
                createRow(1, "2023-11-13 10:50:00.123456", Insert, "1")),
                TEST_DATA_SCHEMA
        );

        when(dataProvider.getBatchSourceData(spark, Collections.singletonList("raw-t1-file1"))).thenReturn(dataset);

        underTest.runJob(spark);

        verify(reloadDiffProcessor, times(1)).createDiff(
                eq(createSourceReference(s1T1)),
                eq(TEMP_RELOAD_PATH + OUTPUT_FOLDER),
                rawDatasetCaptor.capture(),
                archiveDatasetCaptor.capture(),
                eq(dmsTaskStartTime)
        );

        assertThat(
                rawDatasetCaptor.getValue().collectAsList(),
                containsTheSameElementsInOrderAs(dataset.collectAsList())
        );

        Dataset<Row> actualArchiveDataset = archiveDatasetCaptor.getValue();
        assertThat(actualArchiveDataset.collectAsList(), is(empty()));
        assertThat(actualArchiveDataset.schema(), is(equalTo(TEST_DATA_SCHEMA)));
    }

    @Test
    void shouldNotFailWhenThereAreNoRawFiles() {
        Date dmsTaskStartTime = new Date();

        when(arguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(arguments.getRawS3Path()).thenReturn(RAW_PATH);
        when(arguments.getRawArchiveS3Path()).thenReturn(ARCHIVE_PATH);
        when(dmsOrchestrationService.getTaskStartTime(DMS_TASK_ID)).thenReturn(dmsTaskStartTime);
        when(tableDiscoveryService.discoverBatchFilesToLoad(RAW_PATH, spark))
                .thenReturn(Collections.singletonMap(s1T1, Collections.emptyList()));
        when(tableDiscoveryService.discoverBatchFilesToLoad(ARCHIVE_PATH, spark)).thenReturn(discoveredArchivePathsByTable);

        assertDoesNotThrow(() -> underTest.runJob(spark));
    }

    @Test
    void shouldFailWhenUnableToRetrieveSourceReference() {
        Date dmsTaskStartTime = new Date();

        when(arguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(arguments.getRawS3Path()).thenReturn(RAW_PATH);
        when(arguments.getRawArchiveS3Path()).thenReturn(ARCHIVE_PATH);
        when(dmsOrchestrationService.getTaskStartTime(DMS_TASK_ID)).thenReturn(dmsTaskStartTime);
        when(tableDiscoveryService.discoverBatchFilesToLoad(RAW_PATH, spark)).thenReturn(discoveredRawPathsByTable);
        when(tableDiscoveryService.discoverBatchFilesToLoad(ARCHIVE_PATH, spark)).thenReturn(discoveredArchivePathsByTable);
        when(sourceReferenceService.getSourceReference(any(), any())).thenReturn(Optional.empty());

        assertThrows(SchemaNotFoundException.class, () -> underTest.runJob(spark));
    }

    @Test
    void shouldFailWhenAnErrorOccursWhenDiscoveringRawFiles() {
        Date dmsTaskStartTime = new Date();

        when(arguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(arguments.getRawS3Path()).thenReturn(RAW_PATH);
        when(dmsOrchestrationService.getTaskStartTime(DMS_TASK_ID)).thenReturn(dmsTaskStartTime);
        when(tableDiscoveryService.discoverBatchFilesToLoad(RAW_PATH, spark)).thenThrow(new TableDiscoveryException(new Exception()));

        assertThrows(TableDiscoveryException.class, () -> underTest.runJob(spark));
    }

    @Test
    void shouldFailWhenAnErrorOccursWhenDiscoveringArchivedFiles() {
        Date dmsTaskStartTime = new Date();

        when(arguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(arguments.getRawS3Path()).thenReturn(RAW_PATH);
        when(arguments.getRawArchiveS3Path()).thenReturn(ARCHIVE_PATH);
        when(dmsOrchestrationService.getTaskStartTime(DMS_TASK_ID)).thenReturn(dmsTaskStartTime);
        when(tableDiscoveryService.discoverBatchFilesToLoad(RAW_PATH, spark)).thenReturn(discoveredRawPathsByTable);
        when(tableDiscoveryService.discoverBatchFilesToLoad(ARCHIVE_PATH, spark)).thenThrow(new TableDiscoveryException(new Exception()));

        assertThrows(TableDiscoveryException.class, () -> underTest.runJob(spark));
    }

    private void mockSourceReference(ImmutablePair<String, String> sourceTable) {
        SourceReference sourceReference = createSourceReference(sourceTable);
        when(sourceReferenceService.getSourceReference(sourceTable.left, sourceTable.right))
                .thenReturn(Optional.of(sourceReference));
    }

    @NotNull
    private static SourceReference createSourceReference(ImmutablePair<String, String> sourceTable) {
        return new SourceReference(
                sourceTable.left + "_" + sourceTable.right,
                "prisons",
                sourceTable.left,
                sourceTable.right,
                new SourceReference.PrimaryKey(PRIMARY_KEY_COLUMN),
                "some-schema-version-v1",
                TEST_DATA_SCHEMA,
                new SourceReference.SensitiveColumns(Collections.emptyList())
        );
    }

    @NotNull
    private List<Row> collectRecords(List<Dataset<Row>> datasets) {
        return datasets.stream()
                .map(Dataset::collectAsList)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}
