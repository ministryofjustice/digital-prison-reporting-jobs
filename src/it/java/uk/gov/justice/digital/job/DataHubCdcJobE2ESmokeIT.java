package uk.gov.justice.digital.job;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.job.cdc.TableStreamingQuery;
import uk.gov.justice.digital.job.cdc.TableStreamingQueryProvider;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

/**
 * Runs the app as close to end-to-end as possible in an in-memory test as a smoke test and entry point for debugging.
 * Differences to real app runs are:
 * * Using the same minimal test schema for all tables.
 * * Mocking some classes including JobArguments, SourceReferenceService, SourceReference.
 * * Using the file system instead of S3.
 * * Using a test SparkSession.
 */
@ExtendWith(MockitoExtension.class)
public class DataHubCdcJobE2ESmokeIT extends E2ETestBase {
    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReferenceService sourceReferenceService;
    private DataHubCdcJob underTest;
    private List<TableStreamingQuery> streamingQueries;

    @BeforeEach
    public void setUp() throws IOException {
        givenPathsAreConfigured(arguments);
        givenPathsExist();
        givenRetrySettingsAreConfigured(arguments);
        givenDependenciesAreInjected();
        givenASourceReferenceFor(agencyInternalLocationsTable, sourceReferenceService);
        givenASourceReferenceFor(agencyLocationsTable, sourceReferenceService);
        givenASourceReferenceFor(movementReasonsTable, sourceReferenceService);
        givenASourceReferenceFor(offenderBookingsTable, sourceReferenceService);
        givenASourceReferenceFor(offenderExternalMovementsTable, sourceReferenceService);
        givenASourceReferenceFor(offendersTable, sourceReferenceService);
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

        thenEventually(() -> thenCuratedAndStructuredForTableContainForPK(agencyInternalLocationsTable, "1b", 1));
        thenEventually(() -> thenCuratedAndStructuredForTableContainForPK(agencyLocationsTable, "1b", 1));

        thenEventually(() -> thenCuratedAndStructuredForTableDoNotContainPK(movementReasonsTable, 2));
        thenEventually(() -> thenCuratedAndStructuredForTableDoNotContainPK(offenderBookingsTable, 2));

        thenEventually(() -> thenCuratedAndStructuredForTableContainForPK(offenderExternalMovementsTable, "3a", 3));
        thenEventually(() -> thenCuratedAndStructuredForTableContainForPK(offendersTable, "3a", 3));
    }

    private void whenTheJobRuns() {
        streamingQueries = underTest.runJob(spark);
        assertFalse(streamingQueries.isEmpty());
    }


    private void givenDependenciesAreInjected() {
        // Manually creating dependencies because Micronaut test injection is not working
        JobProperties jobProperties = new JobProperties();
        SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
        TableDiscoveryService tableDiscoveryService = new TableDiscoveryService(arguments);
        S3DataProvider s3DataProvider = new S3DataProvider(arguments);
        DataStorageService storageService = new DataStorageService(arguments);
        ViolationService violationService = new ViolationService(arguments, storageService);
        ValidationService validationService = new ValidationService(violationService);
        CdcBatchProcessor batchProcessor = new CdcBatchProcessor(violationService, validationService, storageService);
        TableStreamingQueryProvider tableStreamingQueryProvider = new TableStreamingQueryProvider(arguments, s3DataProvider, batchProcessor, sourceReferenceService);
        underTest = new DataHubCdcJob(arguments, jobProperties, sparkSessionProvider, tableStreamingQueryProvider, tableDiscoveryService);
    }

    protected void givenPathsAreConfigured(JobArguments arguments) {
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
        // Pattern for data written by Spark in tests instead of by DMS
        when(arguments.getCdcFileGlobPattern()).thenReturn("*.parquet");
    }

    private void thenEventuallyCuratedAndStructuredHaveDataForPK(String data, int primaryKey) throws Throwable {
        thenEventually(() -> thenCuratedAndStructuredForTableContainForPK(agencyInternalLocationsTable, data, primaryKey));
        thenEventually(() -> thenCuratedAndStructuredForTableContainForPK(agencyLocationsTable, data, primaryKey));
        thenEventually(() -> thenCuratedAndStructuredForTableContainForPK(movementReasonsTable, data, primaryKey));
        thenEventually(() -> thenCuratedAndStructuredForTableContainForPK(offenderExternalMovementsTable, data, primaryKey));
        thenEventually(() -> thenCuratedAndStructuredForTableContainForPK(offenderBookingsTable, data, primaryKey));
        thenEventually(() -> thenCuratedAndStructuredForTableContainForPK(offendersTable, data, primaryKey));
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