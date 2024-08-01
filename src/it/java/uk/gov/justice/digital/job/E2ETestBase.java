package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;

public class E2ETestBase extends BaseSparkTest {
    protected static final String configurationSchemaName = "configuration";
    protected static final String loadingSchemaName = "loading";
    protected static final String namespace = "prisons";
    protected static final String inputSchemaName = "nomis";
    protected static final String configurationTableName = "datahub_managed_tables";
    protected static final String agencyInternalLocationsTable = "agency_internal_locations";
    protected static final String agencyLocationsTable = "agency_locations";
    protected static final String movementReasonsTable = "movement_reasons";
    protected static final String offenderBookingsTable = "offender_bookings";
    protected static final String offenderExternalMovementsTable = "offender_external_movements";
    protected static final String offendersTable = "offenders";
    protected static final String TEST_CONFIG_KEY = "test-config-key";

    @TempDir
    protected Path testRoot;
    protected String rawPath;
    protected String rawArchivePath;
    protected String structuredPath;
    protected String curatedPath;
    protected String violationsPath;
    protected String tempReloadPath;

    protected String checkpointPath;

    protected static String operationalDataStoreTableName(String tableName) {
        return format("%s.%s_%s", namespace, inputSchemaName, tableName);
    }

    protected void givenPathsAreConfigured(JobArguments arguments) {
        rawPath = testRoot.resolve("raw").toAbsolutePath().toString();
        rawArchivePath = testRoot.resolve("rawArchive").toAbsolutePath().toString();
        structuredPath = testRoot.resolve("structured").toAbsolutePath().toString();
        curatedPath = testRoot.resolve("curated").toAbsolutePath().toString();
        violationsPath = testRoot.resolve("violations").toAbsolutePath().toString();
        tempReloadPath = testRoot.resolve("tempReload").toAbsolutePath().toString();
        when(arguments.getRawS3Path()).thenReturn(rawPath);
        lenient().when(arguments.getRawArchiveS3Path()).thenReturn(rawArchivePath);
        lenient().when(arguments.getStructuredS3Path()).thenReturn(structuredPath);
        lenient().when(arguments.getCuratedS3Path()).thenReturn(curatedPath);
        lenient().when(arguments.getViolationsS3Path()).thenReturn(violationsPath);
        lenient().when(arguments.getTempReloadS3Path()).thenReturn(tempReloadPath);
    }

    protected void givenTableConfigIsConfigured(JobArguments jobArguments, ConfigService configService) {
        when(jobArguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(configService.getConfiguredTables(eq(TEST_CONFIG_KEY))).thenReturn(
                ImmutableSet.of(
                        ImmutablePair.of(inputSchemaName, agencyInternalLocationsTable),
                        ImmutablePair.of(inputSchemaName, agencyLocationsTable),
                        ImmutablePair.of(inputSchemaName, movementReasonsTable),
                        ImmutablePair.of(inputSchemaName, offenderBookingsTable),
                        ImmutablePair.of(inputSchemaName, offenderExternalMovementsTable),
                        ImmutablePair.of(inputSchemaName, offendersTable)
                )
        );
    }

    protected void givenASourceReferenceFor(String inputTableName, SourceReferenceService sourceReferenceService) {
        SourceReference sourceReference = mock(SourceReference.class);
        when(sourceReferenceService.getSourceReference(eq(inputSchemaName), eq(inputTableName))).thenReturn(Optional.of(sourceReference));
        lenient().when(sourceReference.getNamespace()).thenReturn(namespace);
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);
        lenient().when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
    }

    protected void givenNoSourceReferenceFor(String inputTableName, SourceReferenceService sourceReferenceService) {
        when(sourceReferenceService.getSourceReference(eq(inputSchemaName), eq(inputTableName))).thenReturn(Optional.empty());
    }

    protected void givenRawDataIsAddedToEveryTable(List<Row> data) {
        whenDataIsAddedToPathForTable(agencyInternalLocationsTable, data, rawPath);
        whenDataIsAddedToPathForTable(agencyLocationsTable, data, rawPath);
        whenDataIsAddedToPathForTable(movementReasonsTable, data, rawPath);
        whenDataIsAddedToPathForTable(offenderBookingsTable, data, rawPath);
        whenDataIsAddedToPathForTable(offenderExternalMovementsTable, data, rawPath);
        whenDataIsAddedToPathForTable(offendersTable, data, rawPath);
    }

    protected void givenArchiveDataIsAddedToEveryTable(List<Row> data) {
        whenDataIsAddedToPathForTable(agencyInternalLocationsTable, data, rawArchivePath);
        whenDataIsAddedToPathForTable(agencyLocationsTable, data, rawArchivePath);
        whenDataIsAddedToPathForTable(movementReasonsTable, data, rawArchivePath);
        whenDataIsAddedToPathForTable(offenderBookingsTable, data, rawArchivePath);
        whenDataIsAddedToPathForTable(offenderExternalMovementsTable, data, rawArchivePath);
        whenDataIsAddedToPathForTable(offendersTable, data, rawArchivePath);
    }

    protected void givenDestinationTableExists(String tableName, Connection testQueryConnection) throws SQLException {
        try(Statement statement = testQueryConnection.createStatement()) {
            statement.execute(format("CREATE TABLE IF NOT EXISTS %s (pk INTEGER, data VARCHAR)", operationalDataStoreTableName(tableName)));
        }
    }

    protected void whenDataIsAddedToPathForTable(String table, List<Row> inputData, String path) {
        String tablePath = Paths.get(path).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        spark
                .createDataFrame(inputData, TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS)
                .write()
                .mode(SaveMode.Append)
                .parquet(tablePath);
    }

    protected void whenInsertOccursForTableAndPK(String table, int primaryKey, String data, String timestamp) {
        List<Row> input = Collections.singletonList(
                createRow(primaryKey, timestamp, Insert, data)
        );
        whenDataIsAddedToPathForTable(table, input, rawPath);
    }

    protected void whenUpdateOccursForTableAndPK(String table, int primaryKey, String data, String timestamp) {
        List<Row> input = Collections.singletonList(
                createRow(primaryKey, timestamp, Update, data)
        );
        whenDataIsAddedToPathForTable(table, input, rawPath);
    }

    protected void whenDeleteOccursForTableAndPK(String table, int primaryKey, String timestamp) {
        List<Row> input = Collections.singletonList(
                createRow(primaryKey, timestamp, Delete, null)
        );
        whenDataIsAddedToPathForTable(table, input, rawPath);
    }

    protected void thenStructuredViolationsContainsForPK(String table, String data, int primaryKey) {
        String violationsTablePath = Paths.get(violationsPath).resolve("structured").resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        assertViolationsTableContainsForPK(violationsTablePath, data, primaryKey);
    }
}
