package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreConnectionDetailsService;
import uk.gov.justice.digital.test.InMemoryOperationalDataStore;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.test.MinimalTestData.DATA_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;

public class E2ETestBase extends BaseSparkTest {

    protected static final InMemoryOperationalDataStore operationalDataStore = new InMemoryOperationalDataStore();
    private static Connection testQueryConnection;

    protected static final String loadingSchemaName = "loading";
    protected static final String inputSchemaName = "nomis";
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
    protected String structuredPath;
    protected String curatedPath;
    protected String violationsPath;

    protected String checkpointPath;

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

    protected void givenPathsAreConfigured(JobArguments arguments) {
        rawPath = testRoot.resolve("raw").toAbsolutePath().toString();
        structuredPath = testRoot.resolve("structured").toAbsolutePath().toString();
        curatedPath = testRoot.resolve("curated").toAbsolutePath().toString();
        violationsPath = testRoot.resolve("violations").toAbsolutePath().toString();
        when(arguments.getRawS3Path()).thenReturn(rawPath);
        when(arguments.getStructuredS3Path()).thenReturn(structuredPath);
        when(arguments.getCuratedS3Path()).thenReturn(curatedPath);
        when(arguments.getViolationsS3Path()).thenReturn(violationsPath);
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
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);
        lenient().when(sourceReference.getFullyQualifiedTableName()).thenReturn(format("%s.%s", inputSchemaName, inputTableName));
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
    }

    protected void givenNoSourceReferenceFor(String inputTableName, SourceReferenceService sourceReferenceService) {
        when(sourceReferenceService.getSourceReference(eq(inputSchemaName), eq(inputTableName))).thenReturn(Optional.empty());
    }

    protected void givenRawDataIsAddedToEveryTable(List<Row> initialDataEveryTable) {
        whenDataIsAddedToRawForTable(agencyInternalLocationsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(agencyLocationsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(movementReasonsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(offenderBookingsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(offenderExternalMovementsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(offendersTable, initialDataEveryTable);
    }

    protected void givenDestinationTableExists(String tableName) throws SQLException {
        Properties jdbcProps = new Properties();
        jdbcProps.put("user", operationalDataStore.getUsername());
        jdbcProps.put("password", operationalDataStore.getPassword());
        try(Statement statement = testQueryConnection.createStatement()) {
            statement.execute(format("CREATE TABLE IF NOT EXISTS %s.%s (pk INTEGER, data VARCHAR)", inputSchemaName, tableName));
        }
    }

    protected void whenDataIsAddedToRawForTable(String table, List<Row> inputData) {
        String tablePath = Paths.get(rawPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
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
        whenDataIsAddedToRawForTable(table, input);
    }

    protected void whenUpdateOccursForTableAndPK(String table, int primaryKey, String data, String timestamp) {
        List<Row> input = Collections.singletonList(
                createRow(primaryKey, timestamp, Update, data)
        );
        whenDataIsAddedToRawForTable(table, input);
    }

    protected void whenDeleteOccursForTableAndPK(String table, int primaryKey, String timestamp) {
        List<Row> input = Collections.singletonList(
                createRow(primaryKey, timestamp, Delete, null)
        );
        whenDataIsAddedToRawForTable(table, input);
    }

    protected void thenStructuredCuratedAndOperationalDataStoreContainForPK(String table, String data, int primaryKey) throws SQLException {
        thenStructuredAndCuratedForTableContainForPK(table, data, primaryKey);
        thenOperationalDataStoreContainsForPK(table, data, primaryKey);
    }

    protected void thenStructuredAndCuratedForTableContainForPK(String table, String data, int primaryKey) {
        assertStructuredAndCuratedForTableContainForPK(structuredPath, curatedPath, inputSchemaName, table, data, primaryKey);
    }

    protected void thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(String table, int primaryKey) throws SQLException {
        thenStructuredAndCuratedForTableDoNotContainPK(table, primaryKey);
        thenOperationalDataStoreDoesNotContainPK(table, primaryKey);
    }

    protected void thenStructuredAndCuratedForTableDoNotContainPK(String table, int primaryKey) {
        assertStructuredAndCuratedForTableDoNotContainPK(structuredPath, curatedPath, inputSchemaName, table, primaryKey);
    }

    protected void thenStructuredViolationsContainsForPK(String table, String data, int primaryKey) {
        String violationsTablePath = Paths.get(violationsPath).resolve("structured").resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        assertViolationsTableContainsForPK(violationsTablePath, data, primaryKey);
    }

    protected void givenSchemaExists(String schemaName) throws SQLException {
        Properties jdbcProps = new Properties();
        jdbcProps.put("user", operationalDataStore.getUsername());
        jdbcProps.put("password", operationalDataStore.getPassword());
        try(Statement statement = testQueryConnection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
    }

    void givenDatastoreCredentials(OperationalDataStoreConnectionDetailsService connectionDetailsService) {
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

    protected void thenOperationalDataStoreContainsForPK(String table, String data, int primaryKey) throws SQLException {
        String sql = format("SELECT COUNT(1) AS cnt FROM %s.%s WHERE %s = %d AND %s = '%s'",
                inputSchemaName, table, PRIMARY_KEY_COLUMN, primaryKey, DATA_COLUMN, data);
        try(Statement statement = testQueryConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            if(resultSet.next()) {
                int count = resultSet.getInt(1);
                assertEquals(1, count);
            }
        }
    }

    protected void thenOperationalDataStoreDoesNotContainPK(String table, int primaryKey) throws SQLException {
        try {
            String sql = format("SELECT COUNT(1) AS cnt FROM %s.%s WHERE %s = %d",
                    inputSchemaName, table, PRIMARY_KEY_COLUMN, primaryKey);
            try (Statement statement = testQueryConnection.createStatement()) {
                ResultSet resultSet = statement.executeQuery(sql);
                if (resultSet.next()) {
                    int count = resultSet.getInt(1);
                    assertEquals(0, count);
                }
            }
        } catch (SQLException e) {
            // If the table doesn't exist then that is fine and it doesn't contain the primary key
            if(!(e.getMessage().contains("Table") && e.getMessage().contains("not found"))) {
                throw e;
            }
        }
    }

}
