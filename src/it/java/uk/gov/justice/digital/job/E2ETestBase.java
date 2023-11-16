package uk.gov.justice.digital.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;

public class E2ETestBase extends BaseSparkTest {

    protected static final String inputSchemaName = "OMS_OWNER";
    protected static final String agencyInternalLocationsTable = "AGENCY_INTERNAL_LOCATIONS";
    protected static final String agencyLocationsTable = "AGENCY_LOCATIONS";
    protected static final String movementReasonsTable = "MOVEMENT_REASONS";
    protected static final String offenderBookingsTable = "OFFENDER_BOOKINGS";
    protected static final String offenderExternalMovementsTable = "OFFENDER_EXTERNAL_MOVEMENTS";
    protected static final String offendersTable = "OFFENDERS";

    @TempDir
    protected Path testRoot;
    protected String rawPath;
    protected String structuredPath;
    protected String curatedPath;
    protected String violationsPath;

    protected String checkpointPath;

    protected void givenPathsExist() throws IOException {
        createTableDirectories(rawPath);
        createTableDirectories(structuredPath);
        createTableDirectories(curatedPath);
        Files.createDirectories(Paths.get(violationsPath));
        Files.createDirectories(Paths.get(checkpointPath));
    }

    protected void createTableDirectories(String rootPath) throws IOException {
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(agencyInternalLocationsTable));
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(agencyLocationsTable));
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(offenderBookingsTable));
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(movementReasonsTable));
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(offenderExternalMovementsTable));
        Files.createDirectories(Paths.get(rootPath).resolve(inputSchemaName).resolve(offendersTable));
    }

    protected void givenRetrySettingsAreConfigured(JobArguments arguments) {
        when(arguments.getDataStorageRetryMinWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT);
        when(arguments.getDataStorageRetryMaxWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT);
        when(arguments.getDataStorageRetryMaxAttempts()).thenReturn(DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT);
        when(arguments.getDataStorageRetryJitterFactor()).thenReturn(DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT);
    }

    protected void givenASourceReferenceFor(String inputTableName, SourceReferenceService sourceReferenceService) {
        SourceReference sourceReference = mock(SourceReference.class);
        when(sourceReferenceService.getSourceReferenceOrThrow(eq(inputSchemaName), eq(inputTableName))).thenReturn(sourceReference);
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(sourceReference.getSchema()).thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
    }

    protected void givenRawDataIsAddedToEveryTable(List<Row> initialDataEveryTable) {
        whenDataIsAddedToRawForTable(agencyInternalLocationsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(agencyLocationsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(movementReasonsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(offenderBookingsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(offenderExternalMovementsTable, initialDataEveryTable);
        whenDataIsAddedToRawForTable(offendersTable, initialDataEveryTable);
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
                RowFactory.create(Integer.toString(primaryKey), timestamp, "I", data)
        );
        whenDataIsAddedToRawForTable(table, input);
    }

    protected void whenUpdateOccursForTableAndPK(String table, int primaryKey, String data, String timestamp) {
        List<Row> input = Collections.singletonList(
                RowFactory.create(Integer.toString(primaryKey), timestamp, "U", data)
        );
        whenDataIsAddedToRawForTable(table, input);
    }

    protected void whenDeleteOccursForTableAndPK(String table, int primaryKey, String timestamp) {
        List<Row> input = Collections.singletonList(
                RowFactory.create(Integer.toString(primaryKey), timestamp, "D", null)
        );
        whenDataIsAddedToRawForTable(table, input);
    }

    protected void thenCuratedAndStructuredForTableContainForPK(String table, String data, int primaryKey) {
        String structuredTablePath = Paths.get(structuredPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        String curatedTablePath = Paths.get(curatedPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        assertDeltaTableContainsForPK(structuredTablePath, data, primaryKey);
        assertDeltaTableContainsForPK(curatedTablePath, data, primaryKey);
    }

    protected void assertDeltaTableContainsForPK(String tablePath, String data, int primaryKey) {
        Dataset<Row> df = spark.read().format("delta").load(tablePath);
        List<Row> result = df
                .select(DATA_COLUMN)
                .where(col(PRIMARY_KEY_COLUMN).equalTo(lit(Integer.toString(primaryKey))))
                .collectAsList();

        List<Row> expected = Collections.singletonList(RowFactory.create(data));
        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    protected void thenCuratedAndStructuredForTableDoNotContainPK(String table, int primaryKey) {
        String structuredTablePath = Paths.get(structuredPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        String curatedTablePath = Paths.get(curatedPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        assertDeltaTableDoesNotContainPK(structuredTablePath, primaryKey);
        assertDeltaTableDoesNotContainPK(curatedTablePath, primaryKey);
    }

    protected void assertDeltaTableDoesNotContainPK(String tablePath, int primaryKey) {
        Dataset<Row> df = spark.read().format("delta").load(tablePath);
        List<Row> result = df
                .select(DATA_COLUMN)
                .where(col(PRIMARY_KEY_COLUMN).equalTo(lit(Integer.toString(primaryKey))))
                .collectAsList();

        assertEquals(0, result.size());
    }

}
