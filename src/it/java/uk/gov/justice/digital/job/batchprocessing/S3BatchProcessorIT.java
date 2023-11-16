package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoadS3;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoadS3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
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
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;

@ExtendWith(MockitoExtension.class)
class S3BatchProcessorIT extends BaseSparkTest {

    private static final String inputSchemaName = "my-schema";
    private static final String inputTableName = "my-table";

    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReferenceService sourceReferenceService;

    private S3BatchProcessor underTest;

    @TempDir
    protected Path testRoot;
    protected String structuredPath;
    protected String curatedPath;
    protected String violationsPath;

    @BeforeEach
    public void setUp() throws IOException {
        givenPathsAreConfigured(arguments);
        givenPathsExist();
        givenRetrySettingsAreConfigured(arguments);
        givenDependenciesAreInjected();
        givenASourceReference();
    }

    @Test
    public void shouldWriteInsertsToStructuredAndCurated() {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                RowFactory.create("1", "2023-11-13 10:50:00.123456", "I", "1"),
                RowFactory.create("2", "2023-11-13 10:50:00.123456", "I", "2"),
                RowFactory.create("3", "2023-11-13 10:50:00.123456", "U", "3"),
                RowFactory.create("4", "2023-11-13 10:50:00.123456", "D", "4")
        ), TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);

        underTest.processBatch(spark, inputSchemaName, inputTableName, input);

        thenCuratedAndStructuredForTableContainForPK(inputTableName, "1", 1);
        thenCuratedAndStructuredForTableContainForPK(inputTableName, "2", 2);

        thenCuratedAndStructuredForTableDoNotContainPK(inputTableName, 3);
        thenCuratedAndStructuredForTableDoNotContainPK(inputTableName, 4);
    }

    @Test
    public void shouldWriteNullsToViolationsForNonNullableColumns() {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                RowFactory.create("1", "2023-11-13 10:50:00.123456", "I", "1"),
                RowFactory.create("2", null, "I", "2"),
                RowFactory.create("3", "2023-11-13 10:50:00.123456", "I", "3")
        ), TEST_DATA_SCHEMA);
        underTest.processBatch(spark, inputSchemaName, inputTableName, input);

        thenCuratedAndStructuredForTableContainForPK(inputTableName, "1", 1);
        thenCuratedAndStructuredForTableContainForPK(inputTableName, "3", 3);

        thenViolationsForTableContainForPK(inputTableName, "2", 2);
        thenCuratedAndStructuredForTableDoNotContainPK(inputTableName, 2);
    }

    private void givenDependenciesAreInjected() {
        DataStorageService storageService = new DataStorageService(arguments);
        ViolationService violationService = new ViolationService(arguments, storageService);
        ValidationService validationService = new ValidationService(violationService);
        StructuredZoneLoadS3 structuredZoneLoadS3 = new StructuredZoneLoadS3(arguments, storageService, violationService);
        CuratedZoneLoadS3 curatedZoneLoad = new CuratedZoneLoadS3(arguments, storageService, violationService);
        underTest = new S3BatchProcessor(structuredZoneLoadS3, curatedZoneLoad, sourceReferenceService, validationService);
    }

    private void givenPathsAreConfigured(JobArguments arguments) {
        structuredPath = testRoot.resolve("structured").toAbsolutePath().toString();
        curatedPath = testRoot.resolve("curated").toAbsolutePath().toString();
        violationsPath = testRoot.resolve("violations").toAbsolutePath().toString();
        when(arguments.getStructuredS3Path()).thenReturn(structuredPath);
        when(arguments.getCuratedS3Path()).thenReturn(curatedPath);
        when(arguments.getViolationsS3Path()).thenReturn(violationsPath);
    }

    private void givenPathsExist() throws IOException {
        Files.createDirectories(Paths.get(structuredPath).resolve(inputSchemaName).resolve(inputSchemaName).resolve(inputTableName));
        Files.createDirectories(Paths.get(curatedPath).resolve(inputSchemaName).resolve(inputSchemaName).resolve(inputTableName));
        Files.createDirectories(Paths.get(violationsPath));
    }

    private void givenASourceReference() {
        SourceReference sourceReference = mock(SourceReference.class);
        when(sourceReferenceService.getSourceReferenceOrThrow(eq(inputSchemaName), eq(S3BatchProcessorIT.inputTableName))).thenReturn(sourceReference);
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(S3BatchProcessorIT.inputTableName);
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(sourceReference.getSchema()).thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
    }

    private void thenCuratedAndStructuredForTableContainForPK(String table, String data, int primaryKey) {
        String structuredTablePath = Paths.get(structuredPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        String curatedTablePath = Paths.get(curatedPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        assertDeltaTableContainsForPK(structuredTablePath, data, primaryKey);
        assertDeltaTableContainsForPK(curatedTablePath, data, primaryKey);
    }

    private void thenViolationsForTableContainForPK(String table,String data, int primaryKey) {
        String violationsTablePath = Paths.get(violationsPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        assertDeltaTableContainsForPK(violationsTablePath, data, primaryKey);
    }

    private void assertDeltaTableContainsForPK(String tablePath, String data, int primaryKey) {
        Dataset<Row> df = spark.read().format("delta").load(tablePath);
        List<Row> result = df
                .select(DATA_COLUMN)
                .where(col(PRIMARY_KEY_COLUMN).equalTo(lit(Integer.toString(primaryKey))))
                .collectAsList();

        List<Row> expected = Collections.singletonList(RowFactory.create(data));
        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    private void thenCuratedAndStructuredForTableDoNotContainPK(String table, int primaryKey) {
        String structuredTablePath = Paths.get(structuredPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        String curatedTablePath = Paths.get(curatedPath).resolve(inputSchemaName).resolve(table).toAbsolutePath().toString();
        assertDeltaTableDoesNotContainPK(structuredTablePath, primaryKey);
        assertDeltaTableDoesNotContainPK(curatedTablePath, primaryKey);
    }

    private void assertDeltaTableDoesNotContainPK(String tablePath, int primaryKey) {
        Dataset<Row> df = spark.read().format("delta").load(tablePath);
        List<Row> result = df
                .select(DATA_COLUMN)
                .where(col(PRIMARY_KEY_COLUMN).equalTo(lit(Integer.toString(primaryKey))))
                .collectAsList();

        assertEquals(0, result.size());
    }

    private void givenRetrySettingsAreConfigured(JobArguments arguments) {
        when(arguments.getDataStorageRetryMinWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT);
        when(arguments.getDataStorageRetryMaxWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT);
        when(arguments.getDataStorageRetryMaxAttempts()).thenReturn(DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT);
        when(arguments.getDataStorageRetryJitterFactor()).thenReturn(DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT);
    }

}