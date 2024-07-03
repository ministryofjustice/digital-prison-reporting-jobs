package uk.gov.justice.digital.job;

import org.apache.spark.sql.Row;
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
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.service.operationaldatastore.ConnectionPoolProvider;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreConnectionDetailsService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreDataAccess;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreServiceImpl;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreTransformation;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDC;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;

/**
 * Runs the app as close to end-to-end as possible in an in-memory test as a smoke test and entry point for debugging.
 * This test is fairly slow to run so additional in depth test cases should be added elsewhere.
 * Differences to real app runs are:
 * * Using the same minimal test schema for all tables.
 * * Mocking some classes including JobArguments, SourceReferenceService, SourceReference.
 * * Using the file system instead of S3.
 * * Using a test SparkSession.
 */
@ExtendWith(MockitoExtension.class)
public class DataHubCdcJobE2ESmokeIT extends E2ETestBase {
    private final int pk1 = 1;
    private final int pk2 = 2;
    private final int pk3 = 3;
    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private ConfigService configService;
    @Mock
    private OperationalDataStoreConnectionDetailsService connectionDetailsService;
    private DataHubCdcJob underTest;
    private List<TableStreamingQuery> streamingQueries;

    @BeforeEach
    public void setUp() throws Exception {
        givenDatastoreCredentials(connectionDetailsService);
        givenSchemaExists(loadingSchemaName);
        givenSchemaExists(inputSchemaName);
        givenPathsAreConfigured(arguments);
        givenTableConfigIsConfigured(arguments, configService);
        givenGlobPatternIsConfigured();
        givenCheckpointsAreConfigured();
        givenRetrySettingsAreConfigured(arguments);
        givenLoadingSchemaIsConfigured();
        givenDependenciesAreInjected();

        givenDestinationTableExists(agencyInternalLocationsTable);
        givenDestinationTableExists(agencyLocationsTable);
        givenDestinationTableExists(movementReasonsTable);
        givenDestinationTableExists(offenderBookingsTable);
        givenDestinationTableExists(offenderExternalMovementsTable);
    }

    @AfterEach
    public void tearDown() {
        streamingQueries.forEach(TableStreamingQuery::stopQuery);
    }

    @Test
    public void shouldRunTheJobEndToEndApplyingSomeCDCMessagesAndWritingViolations() throws Throwable {
        givenASourceReferenceFor(agencyInternalLocationsTable, sourceReferenceService);
        givenASourceReferenceFor(agencyLocationsTable, sourceReferenceService);
        givenASourceReferenceFor(movementReasonsTable, sourceReferenceService);
        givenASourceReferenceFor(offenderBookingsTable, sourceReferenceService);
        givenASourceReferenceFor(offenderExternalMovementsTable, sourceReferenceService);

        // offenders is the only table that has no schema - we expect its data to arrive in violations
        givenNoSourceReferenceFor(offendersTable, sourceReferenceService);

        List<Row> initialDataEveryTable = Arrays.asList(
                createRow(pk1, "2023-11-13 10:00:00.000000", Insert, "1a"),
                createRow(pk2, "2023-11-13 10:00:00.000000", Insert, "2a")
        );

        givenRawDataIsAddedToEveryTable(initialDataEveryTable);

        whenTheJobRuns();

        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(agencyInternalLocationsTable, "1a", pk1));
        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(agencyLocationsTable, "1a", pk1));
        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(movementReasonsTable, "1a", pk1));
        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(offenderExternalMovementsTable, "1a", pk1));
        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(offenderBookingsTable, "1a", pk1));

        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(agencyInternalLocationsTable, "2a", pk2));
        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(agencyLocationsTable, "2a", pk2));
        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(movementReasonsTable, "2a", pk2));
        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(offenderExternalMovementsTable, "2a", pk2));
        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(offenderBookingsTable, "2a", pk2));

        thenEventually(() -> thenOperationalDataStoreContainsForPK(agencyInternalLocationsTable, "1a", pk1));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(agencyLocationsTable, "1a", pk1));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(movementReasonsTable, "1a", pk1));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(offenderExternalMovementsTable, "1a", pk1));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(offenderBookingsTable, "1a", pk1));

        thenEventually(() -> thenOperationalDataStoreContainsForPK(agencyInternalLocationsTable, "2a", pk2));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(agencyLocationsTable, "2a", pk2));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(movementReasonsTable, "2a", pk2));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(offenderExternalMovementsTable, "2a", pk2));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(offenderBookingsTable, "2a", pk2));

        thenEventually(() -> thenStructuredViolationsContainsForPK(offendersTable, "1a", pk1));
        thenEventually(() -> thenStructuredViolationsContainsForPK(offendersTable, "2a", pk2));

        whenUpdateOccursForTableAndPK(agencyInternalLocationsTable, pk1, "1b", "2023-11-13 10:01:00.000000");
        whenUpdateOccursForTableAndPK(agencyLocationsTable, pk1, "1b", "2023-11-13 10:01:00.000000");

        whenDeleteOccursForTableAndPK(movementReasonsTable, pk2, "2023-11-13 10:01:00.000000");
        whenDeleteOccursForTableAndPK(offenderBookingsTable, pk2, "2023-11-13 10:01:00.000000");

        whenInsertOccursForTableAndPK(offenderExternalMovementsTable, pk3, "3a", "2023-11-13 10:01:00.000000");
        whenInsertOccursForTableAndPK(offendersTable, pk3, "3a", "2023-11-13 10:01:00.000000");

        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(agencyInternalLocationsTable, "1b", pk1));
        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(agencyLocationsTable, "1b", pk1));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(agencyInternalLocationsTable, "1b", pk1));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(agencyLocationsTable, "1b", pk1));

        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(movementReasonsTable, pk2));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offenderBookingsTable, pk2));

        thenEventually(() -> thenStructuredAndCuratedForTableContainForPK(offenderExternalMovementsTable, "3a", pk3));
        thenEventually(() -> thenOperationalDataStoreContainsForPK(offenderExternalMovementsTable, "3a", pk3));

        thenEventually(() -> thenStructuredViolationsContainsForPK(offendersTable, "3a", pk3));

        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offendersTable, pk1);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offendersTable, pk2);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offendersTable, pk3);
    }

    private void whenTheJobRuns() {
        streamingQueries = underTest.runJob(spark);
        assertFalse(streamingQueries.isEmpty());
    }


    private void givenDependenciesAreInjected() {
        // Manually creating dependencies because Micronaut test injection is not working
        JobProperties jobProperties = new JobProperties();
        SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
        TableDiscoveryService tableDiscoveryService = new TableDiscoveryService(arguments, configService);
        S3DataProvider dataProvider = new S3DataProvider(arguments);
        DataStorageService storageService = new DataStorageService(arguments);
        ViolationService violationService = new ViolationService(arguments, storageService, dataProvider, tableDiscoveryService);
        ValidationService validationService = new ValidationService(violationService);
        CuratedZoneCDC curatedZone = new CuratedZoneCDC(arguments, violationService, storageService);
        StructuredZoneCDC structuredZone = new StructuredZoneCDC(arguments, violationService, storageService);
        OperationalDataStoreTransformation operationalDataStoreTransformation = new OperationalDataStoreTransformation();
        ConnectionPoolProvider connectionPoolProvider = new ConnectionPoolProvider();
        OperationalDataStoreDataAccess operationalDataStoreDataAccess =
                new OperationalDataStoreDataAccess(connectionDetailsService, connectionPoolProvider);
        OperationalDataStoreService operationalDataStoreService =
                new OperationalDataStoreServiceImpl(arguments, operationalDataStoreTransformation, operationalDataStoreDataAccess);
        CdcBatchProcessor batchProcessor = new CdcBatchProcessor(
                validationService,
                structuredZone,
                curatedZone,
                dataProvider,
                operationalDataStoreService
        );
        TableStreamingQueryProvider tableStreamingQueryProvider = new TableStreamingQueryProvider(
                arguments, dataProvider, batchProcessor, sourceReferenceService, violationService
        );
        underTest = new DataHubCdcJob(arguments, jobProperties, sparkSessionProvider, tableStreamingQueryProvider, tableDiscoveryService);
    }

    private void givenCheckpointsAreConfigured() throws IOException {
        checkpointPath = testRoot.resolve("checkpoints").toAbsolutePath().toString();
        when(arguments.getCheckpointLocation()).thenReturn(checkpointPath);
        Files.createDirectories(Paths.get(checkpointPath));
    }

    private void givenGlobPatternIsConfigured() {
        // Pattern for data written by Spark as input in tests instead of by DMS
        when(arguments.getCdcFileGlobPattern()).thenReturn("*.parquet");
    }

    protected void givenLoadingSchemaIsConfigured() {
        when(arguments.getOperationalDataStoreLoadingSchemaName()).thenReturn(loadingSchemaName);
    }

    @FunctionalInterface
    interface Thunk {
        void apply() throws Exception;
    }

    private static void thenEventually(Thunk thunk) throws Throwable {
        Optional<Throwable> maybeEx = Optional.empty();
        for (int i = 0; i < 120; i++) {
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