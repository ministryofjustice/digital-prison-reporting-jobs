package uk.gov.justice.digital.job;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.job.batchprocessing.S3BatchProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoadS3;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoadS3;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
class DataHubBatchJobE2ESmokeIT extends E2ETestBase {
    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReferenceService sourceReferenceService;
    private DataHubBatchJob underTest;
    @BeforeEach
    public void setUp() {
        givenPathsAreConfigured(arguments);
        givenGlobPatternIsConfigured();
        givenRetrySettingsAreConfigured(arguments);
        givenDependenciesAreInjected();
        givenASourceReferenceFor(agencyInternalLocationsTable, sourceReferenceService);
        givenASourceReferenceFor(agencyLocationsTable, sourceReferenceService);
        givenASourceReferenceFor(movementReasonsTable, sourceReferenceService);
        givenASourceReferenceFor(offenderBookingsTable, sourceReferenceService);
        givenASourceReferenceFor(offenderExternalMovementsTable, sourceReferenceService);
        givenASourceReferenceFor(offendersTable, sourceReferenceService);
    }

    @Test
    public void shouldRunTheJobEndToEndApplyingSomeCDCMessages() throws IOException {
        List<Row> initialDataEveryTable = Arrays.asList(
                RowFactory.create("1", "2023-11-13 10:00:00.000000", "I", "1"),
                RowFactory.create("2", "2023-11-13 10:00:00.000000", "I", "2"),
                RowFactory.create("3", "2023-11-13 10:00:00.000000", "I", "3"),
                RowFactory.create("4", "2023-11-13 10:00:00.000000", "I", "4"),
                RowFactory.create("5", "2023-11-13 10:00:00.000000", "I", "5")
        );

        givenRawDataIsAddedToEveryTable(initialDataEveryTable);

        whenTheJobRuns();

        thenStructuredAndCuratedForTableContainForPK(agencyInternalLocationsTable, "1", 1);
        thenStructuredAndCuratedForTableContainForPK(agencyLocationsTable, "1", 1);
        thenStructuredAndCuratedForTableContainForPK(movementReasonsTable, "1", 1);
        thenStructuredAndCuratedForTableContainForPK(offenderBookingsTable, "1", 1);
        thenStructuredAndCuratedForTableContainForPK(offenderExternalMovementsTable, "1", 1);
        thenStructuredAndCuratedForTableContainForPK(offendersTable, "1", 1);
    }

    private void whenTheJobRuns() throws IOException {
        underTest.runJob(spark);
    }

    private void givenDependenciesAreInjected() {
        // Manually creating dependencies because Micronaut test injection is not working
        JobProperties properties = new JobProperties();
        SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
        TableDiscoveryService tableDiscoveryService = new TableDiscoveryService(arguments);
        DataStorageService storageService = new DataStorageService(arguments);
        ViolationService violationService = new ViolationService(arguments, storageService);
        ValidationService validationService = new ValidationService(violationService);
        StructuredZoneLoadS3 structuredZoneLoadS3 = new StructuredZoneLoadS3(arguments, storageService, violationService);
        CuratedZoneLoadS3 curatedZoneLoad = new CuratedZoneLoadS3(arguments, storageService, violationService);
        S3BatchProcessor batchProcessor = new S3BatchProcessor(structuredZoneLoadS3, curatedZoneLoad, sourceReferenceService, validationService);
        underTest = new DataHubBatchJob(arguments, properties, sparkSessionProvider, tableDiscoveryService, batchProcessor);
    }

    private void givenGlobPatternIsConfigured() {
        // Pattern for data written by Spark as input in tests instead of by DMS
        when(arguments.getBatchLoadFileGlobPattern()).thenReturn("part-*parquet");
    }
}