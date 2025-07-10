package uk.gov.justice.digital.job;

import org.apache.commons.lang.time.DateUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.job.batchprocessing.ReloadDiffProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DmsOrchestrationService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.DataStorageService;

import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;

@ExtendWith(MockitoExtension.class)
public class CreateReloadDiffJobE2ESmokeIT extends E2ETestBase {

    private static final String DMS_TASK_ID = "some-dms-task-id";
    private final Date dmsTaskStartTime = Date.from(Instant.now());

    @Mock
    private JobArguments arguments;
    @Mock
    private DmsOrchestrationService dmsOrchestrationService;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private ConfigService configService;

    private CreateReloadDiffJob underTest;

    @BeforeEach
    public void setUp() {
        givenPathsAreConfigured(arguments);
        givenTableConfigIsConfigured(arguments, configService);
        givenRetrySettingsAreConfigured(arguments);
        when(arguments.shouldUseNowAsCheckpointForReloadJob()).thenReturn(false);
        when(arguments.getTempReloadOutputFolder()).thenReturn("diffs");
        when(arguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(arguments.getBatchLoadFileGlobPattern()).thenReturn("*.parquet");
        when(dmsOrchestrationService.getTaskStartTime(DMS_TASK_ID)).thenReturn(dmsTaskStartTime);
        givenDependenciesAreInjected();
    }

    @Test
    void shouldCreateReloadDiffsForDiscoveredTables() {
        givenASourceReferenceFor(agencyInternalLocationsTable, sourceReferenceService);
        givenASourceReferenceFor(agencyLocationsTable, sourceReferenceService);
        givenASourceReferenceFor(movementReasonsTable, sourceReferenceService);
        givenASourceReferenceFor(offenderBookingsTable, sourceReferenceService);
        givenASourceReferenceFor(offenderExternalMovementsTable, sourceReferenceService);
        givenASourceReferenceFor(offendersTable, sourceReferenceService);

        List<Row> rawDataEveryTable = Arrays.asList(
                createRow(1, "2023-11-13 10:50:00.123456", Insert, "record_1"),
                createRow(2, "2023-11-13 10:50:00.123456", Insert, "record_2_update"),
                createRow(3, "2023-11-13 10:50:00.123456", Insert, "record_3"),
                createRow(4, "2023-11-13 10:50:00.123456", Insert, "record_4")
        );

        List<Row> archiveDataEveryTable = Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "record_1", formatDate(DateUtils.addSeconds(dmsTaskStartTime, -2))),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "record_2", formatDate(DateUtils.addSeconds(dmsTaskStartTime, -2))),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Insert.getName(), "record_3_insert", formatDate(DateUtils.addSeconds(dmsTaskStartTime, -2))),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Delete.getName(), "record_3_delete", formatDate(DateUtils.addSeconds(dmsTaskStartTime, -1))),
                RowFactory.create(5, "2023-11-13 10:50:00.123456", Insert.getName(), "record_5", formatDate(DateUtils.addSeconds(dmsTaskStartTime, -2)))
        );

        givenRawDataIsAddedToEveryTable(rawDataEveryTable);
        givenArchiveDataIsAddedToEveryTable(archiveDataEveryTable);

        whenTheJobRuns();

        List<Row> expectedDiffsToDelete = Collections.singletonList(
                RowFactory.create(5, "2023-11-13 10:50:00.123456", Delete.getName(), "record_5", formatDate(dmsTaskStartTime))
        );

        List<Row> expectedDiffsToInsert = Arrays.asList(
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Insert.getName(), "record_3", formatDate(dmsTaskStartTime)),
                RowFactory.create(4, "2023-11-13 10:50:00.123456", Insert.getName(), "record_4", formatDate(dmsTaskStartTime))
        );

        List<Row> expectedDiffsToUpdate = Collections.singletonList(
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Update.getName(), "record_2_update", formatDate(dmsTaskStartTime))
        );

        List<String> tables = Arrays.asList(agencyInternalLocationsTable,
                agencyLocationsTable,
                movementReasonsTable,
                offenderBookingsTable,
                offenderExternalMovementsTable,
                offendersTable
        );

        tables.forEach(table -> {
            String toDelete = getPathForDiffs("toDelete", table);
            List<Row> actualDiffsToDelete = spark.read().parquet(toDelete).collectAsList();
            assertThat(actualDiffsToDelete, Matchers.containsInAnyOrder(expectedDiffsToDelete.toArray()));

            List<Row> actualDiffsToInsert = spark.read().parquet(getPathForDiffs("toInsert", table)).collectAsList();
            assertThat(actualDiffsToInsert, Matchers.containsInAnyOrder(expectedDiffsToInsert.toArray()));

            List<Row> actualDiffsToUpdate = spark.read().parquet(getPathForDiffs("toUpdate", table)).collectAsList();
            assertThat(actualDiffsToUpdate, Matchers.containsInAnyOrder(expectedDiffsToUpdate.toArray()));
        });
    }

    @NotNull
    private String getPathForDiffs(String diffOperation, String table) {
        return Paths.get(tempReloadPath)
                .resolve("diffs")
                .resolve(diffOperation)
                .resolve(inputSchemaName)
                .resolve(table)
                .toAbsolutePath()
                .toString();
    }

    private void whenTheJobRuns() {
        underTest.runJob(spark);
    }

    private void givenDependenciesAreInjected() {
        JobProperties properties = new JobProperties();
        SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
        TableDiscoveryService tableDiscoveryService = new TableDiscoveryService(arguments, configService);
        DataStorageService storageService = new DataStorageService(arguments);
        S3DataProvider dataProvider = new S3DataProvider(arguments);
        ReloadDiffProcessor reloadDiffProcessor = new ReloadDiffProcessor(storageService);
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

    @NotNull
    private static String formatDate(Date date) {
        return new SimpleDateFormat("yyyyMMddHHmmss", Locale.getDefault()).format(date);
    }
}
