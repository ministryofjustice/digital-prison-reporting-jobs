package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
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
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.BackfillException;
import uk.gov.justice.digital.exception.SchemaNotFoundException;
import uk.gov.justice.digital.job.batchprocessing.ArchiveBackfillProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.reset;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.CHECKPOINT_COL_VALUE;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.TestHelpers.containsTheSameElementsInOrderAs;

@ExtendWith(MockitoExtension.class)
class ArchiveBackfillJobTest extends SparkTestBase {
    private static final String TEST_CONFIG_KEY = "test-domain";
    private static final String ARCHIVE_PATH = "s3://raw-archive-bucket/";
    private static final String TEMP_RELOAD_PATH = "s3://temp-reload-bucket/";
    private static final String OUTPUT_FOLDER = "temp-reload-output";
    private static final String PATH_1 = "archive-t1-file1";
    private static final String PATH_2 = "archive-t2-file1";
    private static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();

    private static final ImmutablePair<String, String> s1T1 = ImmutablePair.of("s1", "t1");
    private static final ImmutablePair<String, String> s2T2 = ImmutablePair.of("s2", "t2");
    private static final ImmutablePair<String, String> s3T3 = ImmutablePair.of("s3", "t3");
    private static final ImmutablePair<String, String> s4T4 = ImmutablePair.of("s4", "t4");

    private static final Map<ImmutablePair<String, String>, List<String>> discoveredArchivePathsByTable;

    static {
        discoveredArchivePathsByTable = new HashMap<>();
        discoveredArchivePathsByTable.put(s1T1, Collections.singletonList(PATH_1));
        discoveredArchivePathsByTable.put(s2T2, Collections.singletonList(PATH_2));
    }

    @Mock
    private JobArguments arguments;
    @Mock
    private JobProperties properties;
    @Mock
    private ConfigService configService;
    @Mock
    private S3DataProvider dataProvider;
    @Mock
    private TableDiscoveryService tableDiscoveryService;
    @Mock
    private ArchiveBackfillProcessor archiveBackfillProcessor;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Captor
    private ArgumentCaptor<String> outputPathCaptor;
    @Captor
    private ArgumentCaptor<SourceReference> sourceRefCaptor;
    @Captor
    private ArgumentCaptor<Dataset<Row>> archiveDatasetCaptor;
    private ArchiveBackfillJob underTest;

    @BeforeEach
    void setUp() {
        reset(
                arguments,
                properties,
                configService,
                dataProvider,
                tableDiscoveryService,
                archiveBackfillProcessor,
                sourceReferenceService
        );

        underTest = new ArchiveBackfillJob(
                arguments,
                properties,
                configService,
                dataProvider,
                sparkSessionProvider,
                tableDiscoveryService,
                archiveBackfillProcessor,
                sourceReferenceService
        );
    }

    @Test
    void shouldCreateArchiveBackfillOnlyForDiscoveredArchiveTablesContainedInConfiguredTables() {
        when(arguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(arguments.getRawArchiveS3Path()).thenReturn(ARCHIVE_PATH);
        when(arguments.getTempReloadS3Path()).thenReturn(TEMP_RELOAD_PATH);
        when(arguments.getTempReloadOutputFolder()).thenReturn(OUTPUT_FOLDER);
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(s2T2, s1T1, s3T3);
        when(configService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);
        when(tableDiscoveryService.discoverBatchFilesToLoad(ARCHIVE_PATH, spark)).thenReturn(discoveredArchivePathsByTable);
        mockSourceReferences(configuredTables);

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

        when(dataProvider.getBatchSourceData(spark, Collections.singletonList(PATH_1))).thenReturn(archiveDataset1);
        when(dataProvider.getBatchSourceData(spark, Collections.singletonList(PATH_2))).thenReturn(archiveDataset2);

        underTest.runJob(spark);

        verify(archiveBackfillProcessor, times(2)).createBackfilledArchiveData(
                sourceRefCaptor.capture(),
                outputPathCaptor.capture(),
                archiveDatasetCaptor.capture()
        );

        List<String> expectedOutputPaths = Arrays.asList(TEMP_RELOAD_PATH + OUTPUT_FOLDER, TEMP_RELOAD_PATH + OUTPUT_FOLDER);
        List<SourceReference> expectedSourceRefs = createSourceReferences(ImmutableSet.of(s2T2, s1T1));

        assertThat(outputPathCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedOutputPaths));
        assertThat(sourceRefCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedSourceRefs));
        assertThat(
                collectRecords(archiveDatasetCaptor.getAllValues()),
                containsTheSameElementsInOrderAs(collectRecords(Arrays.asList(archiveDataset2, archiveDataset1)))
        );
    }

    @Test
    void shouldNotCallArchiveBackfillProcessorWhenNoDiscoveredArchiveTableIsInConfiguredTables() {
        when(arguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(arguments.getRawArchiveS3Path()).thenReturn(ARCHIVE_PATH);
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(s3T3, s4T4);
        when(configService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);
        when(tableDiscoveryService.discoverBatchFilesToLoad(ARCHIVE_PATH, spark)).thenReturn(discoveredArchivePathsByTable);
        mockSourceReferences(configuredTables);

        underTest.runJob(spark);

        verifyNoInteractions(archiveBackfillProcessor);
    }

    @Test
    void shouldFailWhenUnableToRetrieveSchemasForDomain() {
        when(arguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(configService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(ImmutableSet.of(s2T2, s1T1, s3T3, s4T4));
        when(sourceReferenceService.getAllSourceReferences(any())).thenReturn(Collections.emptyList());

        assertThrows(SchemaNotFoundException.class, () -> underTest.runJob(spark));
    }

    @Test
    void shouldFailWhenAtLeastOneConfiguredTableHasNoArchivedFilesDiscovered() {
        when(arguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(arguments.getRawArchiveS3Path()).thenReturn(ARCHIVE_PATH);
        when(arguments.getTempReloadS3Path()).thenReturn(TEMP_RELOAD_PATH);
        when(arguments.getTempReloadOutputFolder()).thenReturn(OUTPUT_FOLDER);
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(s2T2, s1T1, s3T3);
        when(configService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);
        when(tableDiscoveryService.discoverBatchFilesToLoad(ARCHIVE_PATH, spark)).thenReturn(Collections.singletonMap(s2T2, Collections.emptyList()));
        mockSourceReferences(configuredTables);

        assertThrows(BackfillException.class, () -> underTest.runJob(spark));

        verifyNoInteractions(archiveBackfillProcessor);
    }

    private void mockSourceReferences(ImmutableSet<ImmutablePair<String, String>> sourceTables) {
        List<SourceReference> sourceReferences = createSourceReferences(sourceTables);
        when(sourceReferenceService.getAllSourceReferences(sourceTables))
                .thenReturn(sourceReferences);
    }

    @NotNull
    private static List<SourceReference> createSourceReferences(ImmutableSet<ImmutablePair<String, String>> sourceTables) {
        return sourceTables.stream().map(table -> new SourceReference(
                table.left + "_" + table.right,
                "prisons",
                table.left,
                table.right,
                new SourceReference.PrimaryKey(PRIMARY_KEY_COLUMN),
                "some-schema-version-v1",
                TEST_DATA_SCHEMA,
                new SourceReference.SensitiveColumns(Collections.emptyList())
        )).collect(Collectors.toList());
    }

    @NotNull
    private List<Row> collectRecords(List<Dataset<Row>> datasets) {
        return datasets.stream()
                .map(Dataset::collectAsList)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}
