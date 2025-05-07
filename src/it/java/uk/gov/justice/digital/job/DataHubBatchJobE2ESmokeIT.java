package uk.gov.justice.digital.job;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.job.batchprocessing.BatchProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreServiceImpl;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreTransformation;
import uk.gov.justice.digital.provider.ConnectionPoolProvider;
import uk.gov.justice.digital.service.JDBCGlueConnectionDetailsService;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreDataAccessService;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreRepository;
import uk.gov.justice.digital.test.InMemoryOperationalDataStore;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.config.JobArguments.OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenDatastoreCredentials;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenSchemaExists;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenTablesToWriteToOperationalDataStore;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenTablesToWriteToOperationalDataStoreTableNameIsConfigured;

/**
 * Runs the app as close to end-to-end as possible in an in-memory test as a smoke test and entry point for debugging.
 * Differences to real app runs are:
 * * Using the same minimal test schema for all tables.
 * * Mocking some classes including JobArguments, SourceReferenceService, SourceReference.
 * * Using the file system instead of S3.
 * * Using a test SparkSession.
 */
@ExtendWith(MockitoExtension.class)
class DataHubBatchJobE2ESmokeIT extends E2ETestBase {
    protected static final InMemoryOperationalDataStore operationalDataStore = new InMemoryOperationalDataStore();
    private static Connection testQueryConnection;

    private static final List<Row> initialDataEveryTable = Arrays.asList(
            createRow(1, "2023-11-13 10:00:00.000000", Insert, "1"),
            createRow(2, "2023-11-13 10:00:00.000000", Insert, "2")
    );

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
    private DataHubBatchJob underTest;

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
    public void setUp() throws SQLException {
        givenDatastoreCredentials(connectionDetailsService, operationalDataStore);
        givenSettingsAreConfigured();
        givenSchemaExists(namespace, testQueryConnection);
        givenSchemaExists(configurationSchemaName, testQueryConnection);
        givenTablesToWriteToOperationalDataStoreTableNameIsConfigured(arguments, configurationSchemaName + "." + configurationTableName);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, agencyInternalLocationsTable, testQueryConnection);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, agencyLocationsTable, testQueryConnection);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, movementReasonsTable, testQueryConnection);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, offenderBookingsTable, testQueryConnection);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, offenderExternalMovementsTable, testQueryConnection);

        givenDestinationTableExists(agencyInternalLocationsTable, testQueryConnection);
        givenDestinationTableExists(agencyLocationsTable, testQueryConnection);
        givenDestinationTableExists(movementReasonsTable, testQueryConnection);
        givenDestinationTableExists(offenderBookingsTable, testQueryConnection);
        givenDestinationTableExists(offenderExternalMovementsTable, testQueryConnection);
        givenDependenciesAreInjected();
    }

    @Test
    public void shouldRunTheJobEndToEndApplyingSomeCDCMessagesAndWritingViolations() throws SQLException {
        givenASourceReferenceFor(agencyInternalLocationsTable, sourceReferenceService);
        givenASourceReferenceFor(agencyLocationsTable, sourceReferenceService);
        givenASourceReferenceFor(movementReasonsTable, sourceReferenceService);
        givenASourceReferenceFor(offenderBookingsTable, sourceReferenceService);
        givenASourceReferenceFor(offenderExternalMovementsTable, sourceReferenceService);
        // offenders is the only table that has no schema - we expect its data to arrive in violations
        givenNoSourceReferenceFor(offendersTable, sourceReferenceService);

        givenRawDataIsAddedToEveryTable(initialDataEveryTable);

        whenTheJobRuns();

        thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyInternalLocationsTable, "1", 1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyLocationsTable, "1", 1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(movementReasonsTable, "1", 1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderBookingsTable, "1", 1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderExternalMovementsTable, "1", 1, testQueryConnection);

        thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyInternalLocationsTable, "2", 2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyLocationsTable, "2", 2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(movementReasonsTable, "2", 2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderBookingsTable, "2", 2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderExternalMovementsTable, "2", 2, testQueryConnection);

        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offendersTable, 1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offendersTable, 2, testQueryConnection);
        thenStructuredViolationsContainsForPK(offendersTable, "1", 1);
        thenStructuredViolationsContainsForPK(offendersTable, "2", 2);
    }

    private void givenSettingsAreConfigured() {
        givenPathsAreConfigured(arguments);
        givenTableConfigIsConfigured(arguments, configService);
        givenGlobPatternIsConfigured();
        givenRetrySettingsAreConfigured(arguments);
        when(arguments.getOperationalDataStoreJdbcBatchSize()).thenReturn(OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT);
        when(arguments.getOperationalDataStoreGlueConnectionName()).thenReturn("operational-datastore-connection-name");
        when(properties.getSparkDriverMemory()).thenReturn("2g");
        when(properties.getSparkExecutorMemory()).thenReturn("2g");
    }

    private void whenTheJobRuns() {
        underTest.runJob(spark);
    }

    private void givenDependenciesAreInjected() {
        // Manually creating dependencies because Micronaut test injection is not working
        SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
        TableDiscoveryService tableDiscoveryService = new TableDiscoveryService(arguments, configService);
        DataStorageService storageService = new DataStorageService(arguments);
        S3DataProvider dataProvider = new S3DataProvider(arguments);
        ViolationService violationService = new ViolationService(arguments, storageService, dataProvider, tableDiscoveryService);
        ValidationService validationService = new ValidationService(violationService);
        StructuredZoneLoad structuredZoneLoad = new StructuredZoneLoad(arguments, storageService, violationService);
        CuratedZoneLoad curatedZoneLoad = new CuratedZoneLoad(arguments, storageService, violationService);
        OperationalDataStoreTransformation operationalDataStoreTransformation = new OperationalDataStoreTransformation();
        ConnectionPoolProvider connectionPoolProvider = new ConnectionPoolProvider();
        OperationalDataStoreRepository operationalDataStoreRepository =
                new OperationalDataStoreRepository(arguments, properties, connectionDetailsService, sparkSessionProvider);
        OperationalDataStoreDataAccessService operationalDataStoreDataAccessService =
                new OperationalDataStoreDataAccessService(arguments, connectionDetailsService, connectionPoolProvider, operationalDataStoreRepository);
        OperationalDataStoreService operationalDataStoreService =
                new OperationalDataStoreServiceImpl(arguments, operationalDataStoreTransformation, operationalDataStoreDataAccessService);
        BatchProcessor batchProcessor = new BatchProcessor(structuredZoneLoad, curatedZoneLoad, validationService, operationalDataStoreService);
        underTest = new DataHubBatchJob(
                arguments,
                properties,
                sparkSessionProvider,
                tableDiscoveryService,
                batchProcessor,
                dataProvider,
                sourceReferenceService,
                violationService
        );
    }

    private void givenGlobPatternIsConfigured() {
        // Pattern for data written by Spark as input in tests instead of by DMS
        when(arguments.getBatchLoadFileGlobPattern()).thenReturn("part-*parquet");
    }
}