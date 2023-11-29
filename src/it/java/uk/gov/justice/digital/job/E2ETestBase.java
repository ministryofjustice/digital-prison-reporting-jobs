package uk.gov.justice.digital.job;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;

public class E2ETestBase extends BaseSparkTest {

    protected static final String inputSchemaName = "nomis";
    protected static final String agencyInternalLocationsTable = "agency_internal_locations";
    protected static final String agencyLocationsTable = "agency_locations";
    protected static final String movementReasonsTable = "movement_reasons";
    protected static final String offenderBookingsTable = "offender_bookings";
    protected static final String offenderExternalMovementsTable = "offender_external_movements";
    protected static final String offendersTable = "offenders";

    @TempDir
    protected Path testRoot;
    protected String rawPath;
    protected String structuredPath;
    protected String curatedPath;
    protected String violationsPath;

    protected String checkpointPath;

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

    protected void givenASourceReferenceFor(String inputTableName, SourceReferenceService sourceReferenceService) {
        SourceReference sourceReference = mock(SourceReference.class);
        when(sourceReferenceService.getSourceReference(eq(inputSchemaName), eq(inputTableName))).thenReturn(Optional.of(sourceReference));
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);
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

    protected void thenStructuredAndCuratedForTableContainForPK(String table, String data, int primaryKey) {
        assertStructuredAndCuratedForTableContainForPK(structuredPath, curatedPath, inputSchemaName, table, data, primaryKey);
    }

    protected void thenStructuredAndCuratedForTableDoNotContainPK(String table, int primaryKey) {
        assertStructuredAndCuratedForTableDoNotContainPK(structuredPath, curatedPath, inputSchemaName, table, primaryKey);
    }

    protected void thenViolationsContainsForPK(String table, String data, int primaryKey) {
        String violationsTablePath = Paths.get(violationsPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        assertDeltaTableContainsForPK(violationsTablePath, data, primaryKey);
    }

}
