package uk.gov.justice.digital.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR_RAW;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;
import static uk.gov.justice.digital.test.MinimalTestData.DATA_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.inserts;

@ExtendWith(MockitoExtension.class)
class ViolationServiceIT extends SparkTestBase {

    private static final String source = "some";
    private static final String table = "table";

    private static final String testCaseColumn = "test-case";
    private static final Dataset<Row> original = inserts(spark)
            .withColumn(ERROR, lit("an error"))
            .withColumn(testCaseColumn, lit("original"));
    private static final String extraColumn = "extra-column";
    private static final Dataset<Row> anExtraColumn = original
            .withColumn(extraColumn, lit(extraColumn))
            .withColumn(testCaseColumn, lit(extraColumn));
    private static final Dataset<Row> missingAColumn = original
            .drop(DATA_COLUMN)
            .withColumn(testCaseColumn, lit("missing column"));
    private static final Dataset<Row> stringColumnChangedToInt = original
            .withColumn(DATA_COLUMN, lit(1))
            .withColumn(testCaseColumn, lit("string changed to int"));
    private static final Dataset<Row> intColumnChangedToString = original
            .withColumn(PRIMARY_KEY_COLUMN, lit("a string"))
            .withColumn(testCaseColumn, lit("int changed to string"));
    private static final Dataset<Row> intColumnChangedToLong = original
            .withColumn(PRIMARY_KEY_COLUMN, lit(1L))
            .withColumn(testCaseColumn, lit("int changed to long"));

    private static final String timestampAndIntColumn = "timestamp-and-int";
    private static final Dataset<Row> tsAndInt1 = original
            .withColumn(timestampAndIntColumn, lit(1))
            .withColumn(testCaseColumn, lit("timestamp-and-int-1"));
    private static final Dataset<Row> tsAndInt2 = original
            .withColumn(timestampAndIntColumn, to_timestamp(
                    lit("2022-01-01 00:00:00"),
                    "yyyy-MM-dd HH:mm:ss"
            ))
            .withColumn(testCaseColumn, lit("timestamp-and-int-2"));

    @Mock
    private JobArguments arguments;
    @Mock
    private ConfigService configService;
    @TempDir
    private Path scratchTempDirRoot;
    @TempDir
    private Path rawRoot;
    @TempDir
    private Path violationsRoot;

    private ViolationService underTest;

    @BeforeEach
    void setUp() {
        givenRetrySettingsAreConfigured(arguments);
        underTest = new ViolationService(
                arguments,
                new DataStorageService(arguments),
                new S3DataProvider(arguments),
                new TableDiscoveryService(arguments, configService)
        );
    }

    @Test
    void shouldWriteDataWithIncompatibleSchemasSoThatItCanBeReadBackAgainUsingSpark() {
        when(arguments.getViolationsS3Path()).thenReturn(violationsRoot.toString());

        underTest.handleViolation(spark, original, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, anExtraColumn, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, missingAColumn, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, stringColumnChangedToInt, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, intColumnChangedToString, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, intColumnChangedToLong, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, tsAndInt1, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, tsAndInt2, source, table, STRUCTURED_LOAD);

        Dataset<Row> errorRawAsJson = readStructuredViolationsJson();
        errorRawAsJson.show(30, false);

        long expectedCount = original.count();

        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("original")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("extra-column")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("missing column")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("string changed to int")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("int changed to long")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("int changed to string")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("timestamp-and-int-1")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("timestamp-and-int-2")).count());

        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(OPERATION).dataType());
        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(TIMESTAMP).dataType());
        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(extraColumn).dataType());
        // The columns that have had various data types should widen to String
        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(PRIMARY_KEY_COLUMN).dataType());
        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(DATA_COLUMN).dataType());
        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(timestampAndIntColumn).dataType());
    }

    @Test
    void shouldWriteCdcFilesWithIncompatibleSchemasToViolations() throws Exception {
        when(arguments.getRawS3Path()).thenReturn(rawRoot.toString());
        when(arguments.getViolationsS3Path()).thenReturn(violationsRoot.toString());
        when(arguments.getCdcFileGlobPattern()).thenReturn(JobArguments.CDC_FILE_GLOB_PATTERN_DEFAULT);

        putFilesInRaw(original);
        putFilesInRaw(anExtraColumn);
        putFilesInRaw(missingAColumn);
        putFilesInRaw(stringColumnChangedToInt);
        putFilesInRaw(intColumnChangedToString);
        putFilesInRaw(intColumnChangedToLong);
        putFilesInRaw(tsAndInt1);
        putFilesInRaw(tsAndInt2);
        underTest.writeCdcDataToViolations(spark, source, table, "error msg");

        Dataset<Row> errorRawAsJson = readStructuredViolationsJson();

        long expectedCount = original.count();

        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("original")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("extra-column")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("missing column")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("string changed to int")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("int changed to long")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("int changed to string")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("timestamp-and-int-1")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("timestamp-and-int-2")).count());

    }

    @Test
    void shouldWriteBatchFilesWithIncompatibleSchemasToViolations() throws Exception {
        when(arguments.getRawS3Path()).thenReturn(rawRoot.toString());
        when(arguments.getViolationsS3Path()).thenReturn(violationsRoot.toString());
        when(arguments.getBatchLoadFileGlobPattern()).thenReturn("*-*.parquet");

        putFilesInRaw(original);
        putFilesInRaw(anExtraColumn);
        putFilesInRaw(missingAColumn);
        putFilesInRaw(stringColumnChangedToInt);
        putFilesInRaw(intColumnChangedToString);
        putFilesInRaw(intColumnChangedToLong);
        putFilesInRaw(tsAndInt1);
        putFilesInRaw(tsAndInt2);
        underTest.writeBatchDataToViolations(spark, source, table, "error msg");

        Dataset<Row> errorRawAsJson = readStructuredViolationsJson();

        long expectedCount = original.count();

        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("original")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("extra-column")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("missing column")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("string changed to int")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("int changed to long")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("int changed to string")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("timestamp-and-int-1")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCaseColumn).equalTo("timestamp-and-int-2")).count());

    }

    private Dataset<Row> readStructuredViolationsJson() {
        Dataset<Row> result = spark.read().format("delta")
                .load(violationsRoot.resolve(STRUCTURED_LOAD.getPath()).resolve(source).resolve(table).toString());
        Dataset<Row> errorRawAsJson = spark.read().json(result.select(ERROR_RAW).as(Encoders.STRING()));
        return errorRawAsJson;
    }

    private void putFilesInRaw(Dataset<Row> df) throws IOException {
        df.write().mode(SaveMode.Overwrite).parquet(scratchTempDirRoot.toString());
        File[] parquetFiles = scratchTempDirRoot.toFile().listFiles((dir, name) -> name.endsWith(".parquet"));
        for (File file : parquetFiles) {
            Path destination = rawRoot.resolve(source).resolve(table).resolve(file.getName());
            destination.getParent().toFile().mkdirs();
            Files.move(file.toPath(), destination);
        }
    }
}
