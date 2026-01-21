package uk.gov.justice.digital.job;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
import uk.gov.justice.digital.service.metrics.DisabledMetricReportingService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreServiceImpl;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreTransformation;
import uk.gov.justice.digital.provider.ConnectionPoolProvider;
import uk.gov.justice.digital.service.JDBCGlueConnectionDetailsService;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreDataAccessService;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreRepository;
import uk.gov.justice.digital.test.InMemoryOperationalDataStore;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDC;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.config.JobArguments.OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER;
import static uk.gov.justice.digital.test.Fixtures.fixedClock;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenDatastoreCredentials;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenSchemaExists;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenTablesToWriteToOperationalDataStore;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenTablesToWriteToOperationalDataStoreTableNameIsConfigured;
import static uk.gov.justice.digital.test.SharedTestFunctions.thenEventually;

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
class DataHubCdcJobE2ESmokeIT extends E2ETestBase {
    protected static final InMemoryOperationalDataStore operationalDataStore = new InMemoryOperationalDataStore();
    private static Connection testQueryConnection;

    private final int pk1 = 1;
    private final int pk2 = 2;
    private final int pk3 = 3;
    @Mock
    private JobArguments arguments;
    @Mock
    private JobProperties properties;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private ConfigService configService;
    @Mock
    private JDBCGlueConnectionDetailsService connectionDetailsService;
    private DataHubCdcJob underTest;
    private List<TableStreamingQuery> streamingQueries;

    @BeforeAll
    static void beforeAll() throws Exception {
        operationalDataStore.start();
        testQueryConnection = operationalDataStore.getJdbcConnection();
    }

    @AfterAll
    static void afterAll() throws Exception {
        testQueryConnection.close();
        operationalDataStore.stop();
    }

    @BeforeEach
    void setUp() throws Exception {
        givenDatastoreCredentials(connectionDetailsService, operationalDataStore);
        givenSchemaExists(loadingSchemaName, testQueryConnection);
        givenSchemaExists(namespace, testQueryConnection);
        givenSchemaExists(configurationSchemaName, testQueryConnection);
        givenSettingsAreConfigured();
        givenTablesToWriteToOperationalDataStoreTableNameIsConfigured(arguments, configurationSchemaName + "." + configurationTableName);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, agencyInternalLocationsTable, testQueryConnection);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, agencyLocationsTable, testQueryConnection);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, movementReasonsTable, testQueryConnection);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, offenderBookingsTable, testQueryConnection);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, offenderExternalMovementsTable, testQueryConnection);
        givenDependenciesAreInjected();

        givenDestinationTableExists(agencyInternalLocationsTable, testQueryConnection);
        givenDestinationTableExists(agencyLocationsTable, testQueryConnection);
        givenDestinationTableExists(movementReasonsTable, testQueryConnection);
        givenDestinationTableExists(offenderBookingsTable, testQueryConnection);
        givenDestinationTableExists(offenderExternalMovementsTable, testQueryConnection);
    }

    @AfterEach
    void tearDown() {
        streamingQueries.forEach(TableStreamingQuery::stopQuery);
    }

    @Test
    void shouldRunTheJobEndToEndApplyingSomeCDCMessagesAndWritingViolations() throws Throwable {
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

        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyInternalLocationsTable, "1a", pk1, testQueryConnection));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyLocationsTable, "1a", pk1, testQueryConnection));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(movementReasonsTable, "1a", pk1, testQueryConnection));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderExternalMovementsTable, "1a", pk1, testQueryConnection));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderBookingsTable, "1a", pk1, testQueryConnection));

        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyInternalLocationsTable, "2a", pk2, testQueryConnection));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyLocationsTable, "2a", pk2, testQueryConnection));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(movementReasonsTable, "2a", pk2, testQueryConnection));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderExternalMovementsTable, "2a", pk2, testQueryConnection));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderBookingsTable, "2a", pk2, testQueryConnection));

        thenEventually(() -> thenStructuredViolationsContainsForPK(offendersTable, "1a", pk1));
        thenEventually(() -> thenStructuredViolationsContainsForPK(offendersTable, "2a", pk2));

        whenUpdateOccursForTableAndPK(agencyInternalLocationsTable, pk1, "1b", "2023-11-13 10:01:00.000000");
        whenUpdateOccursForTableAndPK(agencyLocationsTable, pk1, "1b", "2023-11-13 10:01:00.000000");

        whenDeleteOccursForTableAndPK(movementReasonsTable, pk2, "2023-11-13 10:01:00.000000");
        whenDeleteOccursForTableAndPK(offenderBookingsTable, pk2, "2023-11-13 10:01:00.000000");

        whenInsertOccursForTableAndPK(offenderExternalMovementsTable, pk3, "3a", "2023-11-13 10:01:00.000000");
        whenInsertOccursForTableAndPK(offendersTable, pk3, "3a", "2023-11-13 10:01:00.000000");

        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyInternalLocationsTable, "1b", pk1, testQueryConnection));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyLocationsTable, "1b", pk1, testQueryConnection));

        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(movementReasonsTable, pk2, testQueryConnection));
        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offenderBookingsTable, pk2, testQueryConnection));

        thenEventually(() -> thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderExternalMovementsTable, "3a", pk3, testQueryConnection));

        thenEventually(() -> thenStructuredViolationsContainsForPK(offendersTable, "3a", pk3));

        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offendersTable, pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offendersTable, pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offendersTable, pk3, testQueryConnection);
    }

    private void givenSettingsAreConfigured() throws IOException {
        givenPathsAreConfigured(arguments);
        givenTableConfigIsConfigured(arguments, configService);
        givenGlobPatternIsConfigured();
        givenCheckpointsAreConfigured();
        givenRetrySettingsAreConfigured(arguments);
        givenLoadingSchemaIsConfigured();
        when(arguments.getOperationalDataStoreJdbcBatchSize()).thenReturn(OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT);
        when(arguments.streamingJobMaxFilePerTrigger()).thenReturn(STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER);
        when(arguments.getOperationalDataStoreGlueConnectionName()).thenReturn("operational-datastore-connection-name");
        when(properties.getSparkDriverMemory()).thenReturn("2g");
        when(properties.getSparkExecutorMemory()).thenReturn("2g");
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
        DisabledMetricReportingService disabledMetricReportingService = new DisabledMetricReportingService();
        ViolationService violationService =
                new ViolationService(arguments, storageService, dataProvider, tableDiscoveryService, disabledMetricReportingService);
        ValidationService validationService = new ValidationService(violationService);
        CuratedZoneCDC curatedZone = new CuratedZoneCDC(arguments, violationService, storageService);
        StructuredZoneCDC structuredZone = new StructuredZoneCDC(arguments, violationService, storageService);
        OperationalDataStoreTransformation operationalDataStoreTransformation = new OperationalDataStoreTransformation();
        ConnectionPoolProvider connectionPoolProvider = new ConnectionPoolProvider();
        OperationalDataStoreRepository operationalDataStoreRepository =
                new OperationalDataStoreRepository(arguments, properties, connectionDetailsService, sparkSessionProvider);
        OperationalDataStoreDataAccessService operationalDataStoreDataAccessService =
                new OperationalDataStoreDataAccessService(arguments, connectionDetailsService, connectionPoolProvider, operationalDataStoreRepository);
        OperationalDataStoreService operationalDataStoreService =
                new OperationalDataStoreServiceImpl(arguments, operationalDataStoreTransformation, operationalDataStoreDataAccessService);
        CdcBatchProcessor batchProcessor = new CdcBatchProcessor(
                validationService,
                structuredZone,
                curatedZone,
                dataProvider,
                operationalDataStoreService,
                disabledMetricReportingService,
                fixedClock
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

    private void givenLoadingSchemaIsConfigured() {
        when(arguments.getOperationalDataStoreLoadingSchemaName()).thenReturn(loadingSchemaName);
    }
}
