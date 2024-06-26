package uk.gov.justice.digital.job;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;
import uk.gov.justice.digital.job.batchprocessing.BatchProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreConnectionDetailsService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreDataAccess;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreTransformation;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreServiceImpl;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;

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
    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private ConfigService configService;
    @Mock
    private OperationalDataStoreConnectionDetailsService connectionDetailsService;
    private DataHubBatchJob underTest;


    @BeforeEach
    public void setUp() throws SQLException {
        givenDatastoreCredentials();
        givenPathsAreConfigured(arguments);
        givenTableConfigIsConfigured(arguments, configService);
        givenGlobPatternIsConfigured();
        givenRetrySettingsAreConfigured(arguments);
        givenDependenciesAreInjected();
        givenSchemaExists(inputSchemaName);
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

        List<Row> initialDataEveryTable = Arrays.asList(
                createRow(1, "2023-11-13 10:00:00.000000", Insert, "1"),
                createRow(2, "2023-11-13 10:00:00.000000", Insert, "2")
        );

        givenRawDataIsAddedToEveryTable(initialDataEveryTable);

        whenTheJobRuns();

        thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyInternalLocationsTable, "1", 1);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyLocationsTable, "1", 1);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(movementReasonsTable, "1", 1);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderBookingsTable, "1", 1);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderExternalMovementsTable, "1", 1);

        thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyInternalLocationsTable, "2", 2);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(agencyLocationsTable, "2", 2);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(movementReasonsTable, "2", 2);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderBookingsTable, "2", 2);
        thenStructuredCuratedAndOperationalDataStoreContainForPK(offenderExternalMovementsTable, "2", 2);

        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offendersTable, 1);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(offendersTable, 2);
        thenStructuredViolationsContainsForPK(offendersTable, "1", 1);
        thenStructuredViolationsContainsForPK(offendersTable, "2", 2);
    }

    private void whenTheJobRuns() {
        underTest.runJob(spark);
    }

    private void givenDependenciesAreInjected() {
        // Manually creating dependencies because Micronaut test injection is not working
        JobProperties properties = new JobProperties();
        SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
        TableDiscoveryService tableDiscoveryService = new TableDiscoveryService(arguments, configService);
        DataStorageService storageService = new DataStorageService(arguments);
        S3DataProvider dataProvider = new S3DataProvider(arguments);
        ViolationService violationService = new ViolationService(arguments, storageService, dataProvider, tableDiscoveryService);
        ValidationService validationService = new ValidationService(violationService);
        StructuredZoneLoad structuredZoneLoad = new StructuredZoneLoad(arguments, storageService, violationService);
        CuratedZoneLoad curatedZoneLoad = new CuratedZoneLoad(arguments, storageService, violationService);
        OperationalDataStoreTransformation operationalDataStoreTransformation = new OperationalDataStoreTransformation();
        OperationalDataStoreDataAccess operationalDataStoreDataAccess = new OperationalDataStoreDataAccess(connectionDetailsService);
        OperationalDataStoreService operationalDataStoreService =
                new OperationalDataStoreServiceImpl(operationalDataStoreTransformation, operationalDataStoreDataAccess);
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

    private void givenDatastoreCredentials() {
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername(operationalDataStore.getUsername());
        credentials.setPassword(operationalDataStore.getPassword());

        when(connectionDetailsService.getConnectionDetails()).thenReturn(
                new OperationalDataStoreConnectionDetails(
                        operationalDataStore.getJdbcUrl(),
                        operationalDataStore.getDriverClassName(),
                        credentials
                )
        );
    }
}