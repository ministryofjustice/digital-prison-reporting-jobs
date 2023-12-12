package uk.gov.justice.digital.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;

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
public class ViolationServiceIT extends BaseSparkTest {

    private static final String source = "some";
    private static final String table = "table";

    @Mock
    private JobArguments arguments;
    @TempDir
    protected Path violationsRoot;

    private ViolationService underTest;

    @BeforeEach
    public void setUp() {
        givenRetrySettingsAreConfigured(arguments);
        when(arguments.getViolationsS3Path()).thenReturn(violationsRoot.toString());
        underTest = new ViolationService(
                arguments,
                new DataStorageService(arguments),
                new S3DataProvider(arguments),
                new TableDiscoveryService(arguments)
        );
    }

    @Test
    public void shouldWriteDataWithIncompatibleSchemasSoThatItCanBeReadBackAgainUsingSpark() throws Exception {
        String testCase = "marker";
        Dataset<Row> original = inserts(spark)
                .withColumn(ERROR, lit("an error"))
                .withColumn(testCase, lit("original"));
        String extraColumn = "extra-column";
        Dataset<Row> anExtraColumn = original
                .withColumn(extraColumn, lit(extraColumn))
                .withColumn(testCase, lit(extraColumn));
        Dataset<Row> missingAColumn = original
                .drop(DATA_COLUMN)
                .withColumn(testCase, lit("missing column"));
        Dataset<Row> stringColumnChangedToInt = original
                .withColumn(DATA_COLUMN, lit(1))
                .withColumn(testCase, lit("string changed to int"));
        Dataset<Row> intColumnChangedToString = original
                .withColumn(PRIMARY_KEY_COLUMN, lit("a string"))
                .withColumn(testCase, lit("int changed to string"));
        Dataset<Row> intColumnChangedToLong = original
                .withColumn(PRIMARY_KEY_COLUMN, lit(1L))
                .withColumn(testCase, lit("int changed to long"));

        String timestampAndIntColumn = "timestamp-and-int";
        Dataset<Row> tsAndInt1 = original
                .withColumn(timestampAndIntColumn, lit(1))
                .withColumn(testCase, lit("timestamp-and-int-1"));
        Dataset<Row> tsAndInt2 = original
                .withColumn(timestampAndIntColumn, to_timestamp(
                        lit("2022-01-01 00:00:00"),
                        "yyyy-MM-dd HH:mm:ss"
                ))
                .withColumn(testCase, lit("timestamp-and-int-2"));

        underTest.handleViolation(spark, original, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, anExtraColumn, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, missingAColumn, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, stringColumnChangedToInt, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, intColumnChangedToString, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, intColumnChangedToLong, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, tsAndInt1, source, table, STRUCTURED_LOAD);
        underTest.handleViolation(spark, tsAndInt2, source, table, STRUCTURED_LOAD);

        Dataset<Row> result = spark.read().format("delta")
                .load(violationsRoot.resolve(STRUCTURED_LOAD.getPath()).resolve(source).resolve(table).toString());
        Dataset<Row> errorRawAsJson = spark.read().json(result.select(ERROR_RAW).as(Encoders.STRING()));
        errorRawAsJson.show(30, false);

        long expectedCount = original.count();

        assertEquals(expectedCount, errorRawAsJson.where(col(testCase).equalTo("original")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCase).equalTo("extra-column")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCase).equalTo("missing column")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCase).equalTo("string changed to int")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCase).equalTo("int changed to long")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCase).equalTo("int changed to string")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCase).equalTo("timestamp-and-int-1")).count());
        assertEquals(expectedCount, errorRawAsJson.where(col(testCase).equalTo("timestamp-and-int-2")).count());

        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(OPERATION).dataType());
        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(TIMESTAMP).dataType());
        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(extraColumn).dataType());
        // The columns that have had various data types should widen to String
        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(PRIMARY_KEY_COLUMN).dataType());
        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(DATA_COLUMN).dataType());
        assertEquals(DataTypes.StringType, errorRawAsJson.schema().apply(timestampAndIntColumn).dataType());
    }
}
