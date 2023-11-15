package uk.gov.justice.digital.job.cdc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT;
import static uk.gov.justice.digital.test.MinimalTestData.DATA_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.primaryKey;
import static uk.gov.justice.digital.test.MinimalTestData.testDataSchemaNonNullableColumns;

/**
 * Runs the app as close to end-to-end as possible in an in-memory test as a smoke test and entry point for debugging.
 * Differences to real app runs are:
 * * Using the same minimal schema for all tables.
 * * Mocking some classes including JobArguments, SourceReferenceService, SourceReference.
 * * Using the file system instead of S3.
 * * Using a test SparkSession
 */
@ExtendWith(MockitoExtension.class)
public class DataHubCdcJobE2ESmokeIT extends BaseSparkTest {

    private static final String inputSchemaName = "OMS_OWNER";
    private static final String agencyInternalLocationsTable = "AGENCY_INTERNAL_LOCATIONS";
    private static final String agencyLocationsTable = "AGENCY_LOCATIONS";
    private static final String movementReasonsTable = "MOVEMENT_REASONS";
    private static final String offenderBookingsTable = "OFFENDER_BOOKINGS";
    private static final String offenderExternalMovementsTable = "OFFENDER_EXTERNAL_MOVEMENTS";
    private static final String offendersTable = "OFFENDERS";

    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReferenceService sourceReferenceService;
    private DataHubCdcJob underTest;

    @TempDir
    private Path testRoot;
    private String rawPath;
    private String structuredPath;
    private String curatedPath;
    private String violationsPath;
    private String checkpointPath;
    private List<TableStreamingQuery> streamingQueries;

    @BeforeEach
    public void setUp() throws IOException {
        givenPathsAreConfigured();
        givenPathsExist();
        givenRetrySettingsAreConfigured();
        givenDependenciesAreInjected();
        givenASourceReferenceFor(agencyInternalLocationsTable);
        givenASourceReferenceFor(agencyLocationsTable);
        givenASourceReferenceFor(movementReasonsTable);
        givenASourceReferenceFor(offenderBookingsTable);
        givenASourceReferenceFor(offenderExternalMovementsTable);
        givenASourceReferenceFor(offendersTable);
    }

    @AfterEach
    public void tearDown() {
        streamingQueries.forEach(query -> {
            try {
                query.stopQuery();
            } catch (TimeoutException e) {
                // squash
            }
        });
    }

    @Test
    public void shouldRunTheJobEndToEndApplyingSomeCDCMessages() throws Throwable {
        List<Row> initialDataEveryTable = Arrays.asList(
                RowFactory.create("1", "2023-11-13 10:00:00.000000", "I", "1a"),
                RowFactory.create("2", "2023-11-13 10:00:00.000000", "I", "2a")
        );

        givenRawDataIsAddedToEveryTable(initialDataEveryTable);

        whenTheJobRuns();

        thenEventuallyCuratedAndStructuredHaveDataForPK("1a", 1);
        thenEventuallyCuratedAndStructuredHaveDataForPK("2a", 2);

        whenUpdateOccursForTableAndPK(agencyInternalLocationsTable, 1, "1b", "2023-11-13 10:01:00.000000");
        whenUpdateOccursForTableAndPK(agencyLocationsTable, 1, "1b", "2023-11-13 10:01:00.000000");

        whenDeleteOccursForTableAndPK(movementReasonsTable, 2, "2023-11-13 10:01:00.000000");
        whenDeleteOccursForTableAndPK(offenderBookingsTable, 2, "2023-11-13 10:01:00.000000");

        whenInsertOccursForTableAndPK(offenderExternalMovementsTable, 3, "3a", "2023-11-13 10:01:00.000000");
        whenInsertOccursForTableAndPK(offendersTable, 3, "3a", "2023-11-13 10:01:00.000000");

        thenEventually(() -> assertCuratedAndStructuredForTableContainForPK(agencyInternalLocationsTable, "1b", 1));
        thenEventually(() -> assertCuratedAndStructuredForTableContainForPK(agencyLocationsTable, "1b", 1));

        thenEventually(() -> assertCuratedAndStructuredForTableDoNotContainPK(movementReasonsTable, 2));
        thenEventually(() -> assertCuratedAndStructuredForTableDoNotContainPK(offenderBookingsTable, 2));

        thenEventually(() -> assertCuratedAndStructuredForTableContainForPK(offenderExternalMovementsTable, "3a", 3));
        thenEventually(() -> assertCuratedAndStructuredForTableContainForPK(offendersTable, "3a", 3));
    }

    private void givenRawDataIsAddedToEveryTable(List<Row> initialDataEveryTable) {
        whenDataIsAddedToRawForTable(agencyInternalLocationsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(agencyLocationsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(movementReasonsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(offenderBookingsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(offenderExternalMovementsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(offendersTable, initialDataEveryTable);
    }

    private void whenTheJobRuns() {
        streamingQueries = underTest.runJob(spark);
        assertFalse(streamingQueries.isEmpty());
    }

    private void givenPathsAreConfigured() {
        rawPath = testRoot.resolve("raw").toAbsolutePath().toString();
        structuredPath = testRoot.resolve("structured").toAbsolutePath().toString();
        curatedPath = testRoot.resolve("curated").toAbsolutePath().toString();
        violationsPath = testRoot.resolve("violations").toAbsolutePath().toString();
        checkpointPath = testRoot.resolve("checkpoints").toAbsolutePath().toString();
        when(arguments.getRawS3Path()).thenReturn(rawPath);
        when(arguments.getStructuredS3Path()).thenReturn(structuredPath);
        when(arguments.getCuratedS3Path()).thenReturn(curatedPath);
        when(arguments.getViolationsS3Path()).thenReturn(violationsPath);
        when(arguments.getCheckpointLocation()).thenReturn(checkpointPath);
        when(arguments.getCdcFileGlobPattern()).thenReturn("*.parquet");
    }

    private void givenPathsExist() throws IOException {
        createTableDirectories(rawPath);
        createTableDirectories(structuredPath);
        createTableDirectories(curatedPath);
        Files.createDirectories(Paths.get(violationsPath));
        Files.createDirectories(Paths.get(checkpointPath));
    }

    private void createTableDirectories(String rootPath) throws IOException {
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(agencyInternalLocationsTable));
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(agencyLocationsTable));
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(offenderBookingsTable));
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(movementReasonsTable));
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(offenderExternalMovementsTable));
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(offendersTable));
    }

    private void givenRetrySettingsAreConfigured() {
        when(arguments.getDataStorageRetryMinWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT);
        when(arguments.getDataStorageRetryMaxWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT);
        when(arguments.getDataStorageRetryMaxAttempts()).thenReturn(DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT);
        when(arguments.getDataStorageRetryJitterFactor()).thenReturn(DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT);
    }

    private void givenDependenciesAreInjected() {
        // Manually creating dependencies because Micronaut test injection is not working
        JobProperties jobProperties = new JobProperties();
        SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
        TableDiscovery tableDiscovery = new TableDiscovery();
        S3DataProvider s3DataProvider = new S3DataProvider(arguments);
        DataStorageService storageService = new DataStorageService(arguments);
        ViolationService violationService = new ViolationService(arguments, storageService);
        ValidationService validationService = new ValidationService(violationService);
        CdcBatchProcessor batchProcessor = new CdcBatchProcessor(violationService, validationService, storageService);
        TableStreamingQueryProvider tableStreamingQueryProvider = new TableStreamingQueryProvider(arguments, s3DataProvider, batchProcessor, sourceReferenceService);
        underTest = new DataHubCdcJob(arguments, jobProperties, sparkSessionProvider, tableStreamingQueryProvider, tableDiscovery);
    }

    private void givenASourceReferenceFor(String inputTableName) {
        SourceReference sourceReference = mock(SourceReference.class);
        when(sourceReferenceService.getSourceReferenceOrThrow(eq(inputSchemaName), eq(inputTableName))).thenReturn(sourceReference);
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(sourceReference.getSchema()).thenReturn(testDataSchemaNonNullableColumns);
    }
    private void thenEventuallyCuratedAndStructuredHaveDataForPK(String data, int primaryKey) throws Throwable {
        thenEventually(() -> assertCuratedAndStructuredForTableContainForPK(agencyInternalLocationsTable, data, primaryKey));
        thenEventually(() -> assertCuratedAndStructuredForTableContainForPK(agencyLocationsTable, data, primaryKey));
        thenEventually(() -> assertCuratedAndStructuredForTableContainForPK(movementReasonsTable, data, primaryKey));
        thenEventually(() -> assertCuratedAndStructuredForTableContainForPK(offenderExternalMovementsTable, data, primaryKey));
        thenEventually(() -> assertCuratedAndStructuredForTableContainForPK(offenderBookingsTable, data, primaryKey));
        thenEventually(() -> assertCuratedAndStructuredForTableContainForPK(offendersTable, data, primaryKey));
    }


    private void whenInsertOccursForTableAndPK(String table, int primaryKey, String data, String timestamp) {
        List<Row> input = Collections.singletonList(
                RowFactory.create(Integer.toString(primaryKey), timestamp, "I", data)
        );
        whenDataIsAddedToRawForTable(table, input);
    }

    private void whenUpdateOccursForTableAndPK(String table, int primaryKey, String data, String timestamp) {
        List<Row> input = Collections.singletonList(
                RowFactory.create(Integer.toString(primaryKey), timestamp, "U", data)
        );
        whenDataIsAddedToRawForTable(table, input);
    }

    private void whenDeleteOccursForTableAndPK(String table, int primaryKey, String timestamp) {
        List<Row> input = Collections.singletonList(
                RowFactory.create(Integer.toString(primaryKey), timestamp, "D", null)
        );
        whenDataIsAddedToRawForTable(table, input);
    }

    private void whenDataIsAddedToRawForTable(String table, List<Row> inputData) {
        String tablePath = Paths.get(rawPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        spark
                .createDataFrame(inputData, testDataSchemaNonNullableColumns)
                .write()
                .mode(SaveMode.Append)
                .parquet(tablePath);
    }

    private void assertCuratedAndStructuredForTableContainForPK(String table, String data, int primaryKey) {
        String structuredTablePath = Paths.get(structuredPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        String curatedTablePath = Paths.get(curatedPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        assertDeltaTableContainsForPK(structuredTablePath, data, primaryKey);
        assertDeltaTableContainsForPK(curatedTablePath, data, primaryKey);
    }

    private void assertDeltaTableContainsForPK(String tablePath, String data, int primaryKey) {
        Dataset<Row> df = spark.read().format("delta").load(tablePath);
        List<Row> result = df
                .select(DATA_COLUMN)
                .where(col(PRIMARY_KEY_COLUMN).equalTo(lit(Integer.toString(primaryKey))))
                .collectAsList();

        List<Row> expected = Collections.singletonList(RowFactory.create(data));
        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    private void assertCuratedAndStructuredForTableDoNotContainPK(String table, int primaryKey) {
        String structuredTablePath = Paths.get(structuredPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        String curatedTablePath = Paths.get(curatedPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        assertDeltaTableDoesNotContainPK(structuredTablePath, primaryKey);
        assertDeltaTableDoesNotContainPK(curatedTablePath, primaryKey);
    }

    private void assertDeltaTableDoesNotContainPK(String tablePath, int primaryKey) {
        Dataset<Row> df = spark.read().format("delta").load(tablePath);
        List<Row> result = df
                .select(DATA_COLUMN)
                .where(col(PRIMARY_KEY_COLUMN).equalTo(lit(Integer.toString(primaryKey))))
                .collectAsList();

        assertEquals(0, result.size());
    }

    @FunctionalInterface
    interface Thunk {
        void apply() throws Exception;
    }

    private static void thenEventually(Thunk thunk) throws Throwable {
        Optional<Throwable> maybeEx = Optional.empty();
        for (int i = 0; i < 10; i++) {
            try {
                thunk.apply();
                maybeEx = Optional.empty();
                break;
            } catch (Exception | AssertionError e) {
                maybeEx = Optional.of(e);
                TimeUnit.SECONDS.sleep(2);
            }
        }
        if(maybeEx.isPresent()) {
            throw maybeEx.get();
        }
    }
}