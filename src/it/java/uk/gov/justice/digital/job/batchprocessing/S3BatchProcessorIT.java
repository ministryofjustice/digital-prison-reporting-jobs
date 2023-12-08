package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.test.BaseMinimalDataIntegrationTest;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoadS3;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoadS3;

import java.util.Arrays;

import static org.apache.spark.sql.functions.lit;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;

@ExtendWith(MockitoExtension.class)
class S3BatchProcessorIT extends BaseMinimalDataIntegrationTest {
    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReference sourceReference;

    private S3BatchProcessor underTest;

    @BeforeEach
    public void setUp() {
        givenPathsAreConfigured();
        givenRetrySettingsAreConfigured(arguments);
        givenS3BatchProcessorDependenciesAreInjected();
        givenASourceReference();
    }

    @Test
    public void shouldWriteInsertsToStructuredAndCurated() {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, "2023-11-13 10:50:00.123456", Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Update, "data3"),
                createRow(pk4, "2023-11-13 10:50:00.123456", Delete, "data4")
        ), TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);

        underTest.processBatch(spark, sourceReference, input);

        thenStructuredAndCuratedContainForPK("data1", pk1);
        thenStructuredAndCuratedContainForPK("data2", pk2);

        thenStructuredAndCuratedDoNotContainPK(pk3);
        thenStructuredAndCuratedDoNotContainPK(pk4);
    }

    @Test
    public void shouldWriteNullsToViolationsForNonNullableColumns() {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA);
        underTest.processBatch(spark, sourceReference, input);

        thenStructuredAndCuratedContainForPK("data1", pk1);
        thenStructuredAndCuratedContainForPK("data3", pk3);

        thenStructuredViolationsContainsForPK("data2", pk2);
        thenStructuredAndCuratedDoNotContainPK(pk2);
    }

    @Test
    public void shouldWriteToViolationsForDfWithExtraColumn() {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("extra-column", lit(1));
        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredAndCuratedDoNotContainPK(pk1);
        thenStructuredAndCuratedDoNotContainPK(pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);
    }

    @Test
    public void shouldWriteToViolationsForDfWithMissingColumn() {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).drop("data");
        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredAndCuratedDoNotContainPK(pk1);
        thenStructuredAndCuratedDoNotContainPK(pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);
    }

    @Test
    public void shouldWriteToViolationsForDfWhenTypeGoesFromStringToInt() {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("data", lit(1));
        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredAndCuratedDoNotContainPK(pk1);
        thenStructuredAndCuratedDoNotContainPK(pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);
    }

    @Test
    public void shouldWriteToViolationsForDfWhenTypeGoesFromIntToString() {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("data", lit(1));
        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredAndCuratedDoNotContainPK(pk1);
        thenStructuredAndCuratedDoNotContainPK(pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);
    }

    @Test
    public void shouldWriteToViolationsForDfWhenTypeGoesFromIntToLong() {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("data", lit(1L));
        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredAndCuratedDoNotContainPK(pk1);
        thenStructuredAndCuratedDoNotContainPK(pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);
    }

    private void givenS3BatchProcessorDependenciesAreInjected() {
        DataStorageService storageService = new DataStorageService(arguments);
        ViolationService violationService = new ViolationService(arguments, storageService);
        ValidationService validationService = new ValidationService(violationService);
        StructuredZoneLoadS3 structuredZoneLoadS3 = new StructuredZoneLoadS3(arguments, storageService, violationService);
        CuratedZoneLoadS3 curatedZoneLoad = new CuratedZoneLoadS3(arguments, storageService, violationService);
        underTest = new S3BatchProcessor(structuredZoneLoadS3, curatedZoneLoad, validationService);
    }

    private void givenPathsAreConfigured() {
        rawPath = testRoot.resolve("raw").toAbsolutePath().toString();
        structuredPath = testRoot.resolve("structured").toAbsolutePath().toString();
        curatedPath = testRoot.resolve("curated").toAbsolutePath().toString();
        violationsPath = testRoot.resolve("violations").toAbsolutePath().toString();
        when(arguments.getStructuredS3Path()).thenReturn(structuredPath);
        when(arguments.getCuratedS3Path()).thenReturn(curatedPath);
        when(arguments.getViolationsS3Path()).thenReturn(violationsPath);
    }

    private void givenASourceReference() {
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(S3BatchProcessorIT.inputTableName);
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
    }
}