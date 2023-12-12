package uk.gov.justice.digital.job.cdc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Option;
import scala.collection.Seq;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.NoSchemaNoDataException;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.test.BaseMinimalDataIntegrationTest;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDCS3;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDCS3;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.MinimalTestData.encoder;
import static uk.gov.justice.digital.test.SparkTestHelpers.convertListToSeq;


/**
 * This class tests TableStreamingQuery using mainly real dependencies, including integrating with real
 * delta lake operations on the local filesystem. This allows testing the processing logic together with the
 * delta lake merge operations to give a close to end-to-end set of tests for an individual table.
 * The input stream is mocked and uses Spark's MemoryStream. This allows us to simulate test scenarios where test data
 * can be received in a single batch or across multiple batches.
 */
@ExtendWith(MockitoExtension.class)
public class TableStreamingQueryIT extends BaseMinimalDataIntegrationTest {

    @Mock
    private JobArguments arguments;
    @Mock
    private S3DataProvider dataProvider;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private TableDiscoveryService tableDiscoveryService;
    private TableStreamingQuery underTest;
    private MemoryStream<Row> inputStream;
    private StreamingQuery streamingQuery;

    @BeforeEach
    public void setUp() {
        givenPathsAreConfigured();
        givenRetrySettingsAreConfigured(arguments);
    }

    @AfterEach
    public void tearDown() throws TimeoutException {
        streamingQuery.stop();
    }

    @Test
    public void shouldHandleInsertsForMultiplePrimaryKeysInSameBatch() throws Exception {
        givenSourceReference();
        givenASourceReferenceSchema();
        givenASourceReferencePrimaryKey();
        givenAMatchingSchema();
        givenAnInputStream();
        givenTableStreamingQuery();
        givenTheStreamingQueryRuns();

        whenInsertOccursForPK(pk1, "data1", "2023-11-13 10:00:01.000000");
        whenInsertOccursForPK(pk2, "data2", "2023-11-13 10:00:01.000000");
        whenInsertOccursForPK(pk3, "data3", "2023-11-13 10:00:02.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data1", pk1);
        thenStructuredAndCuratedContainForPK("data2", pk2);
        thenStructuredAndCuratedContainForPK("data3", pk3);
    }
    @Test
    public void shouldHandleMultiplePrimaryKeysAcrossBatches() throws Exception {
        givenSourceReference();
        givenASourceReferenceSchema();
        givenASourceReferencePrimaryKey();
        givenAMatchingSchema();
        givenAnInputStream();
        givenTableStreamingQuery();
        givenTheStreamingQueryRuns();

        whenInsertOccursForPK(pk1, "data1a", "2023-11-13 10:00:01.000000");
        whenInsertOccursForPK(pk2, "data2a", "2023-11-13 10:00:01.000000");
        whenInsertOccursForPK(pk3, "data3a", "2023-11-13 10:00:02.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data1a", pk1);
        thenStructuredAndCuratedContainForPK("data2a", pk2);
        thenStructuredAndCuratedContainForPK("data3a", pk3);

        whenUpdateOccursForPK(pk1, "data1b", "2023-11-13 10:01:01.000000");
        whenUpdateOccursForPK(pk2, "data2b", "2023-11-13 10:01:01.000000");
        whenDeleteOccursForPK(pk3, "2023-11-13 10:01:01.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data1b", pk1);
        thenStructuredAndCuratedContainForPK("data2b", pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);
    }

    @Test
    public void shouldHandleInsertFollowedByUpdatesAndDeleteInSameBatchWithDifferentTimestamps() throws Exception {
        givenSourceReference();
        givenASourceReferenceSchema();
        givenASourceReferencePrimaryKey();
        givenAMatchingSchema();
        givenAnInputStream();
        givenTableStreamingQuery();
        givenTheStreamingQueryRuns();

        whenInsertOccursForPK(pk1, "data1a", "2023-11-13 10:00:01.000000");
        whenUpdateOccursForPK(pk1, "data1b", "2023-11-13 10:00:02.000000");
        whenUpdateOccursForPK(pk1, "data1c", "2023-11-13 10:00:03.000000");
        whenDeleteOccursForPK(pk1, "2023-11-13 10:00:04.000000");

        whenInsertOccursForPK(pk2, "data2a", "2023-11-13 10:00:01.000000");
        whenUpdateOccursForPK(pk2, "data2b", "2023-11-13 10:00:02.000000");
        whenUpdateOccursForPK(pk2, "data2c", "2023-11-13 10:00:03.000000");

        whenInsertOccursForPK(pk3, "data3a", "2023-11-13 10:00:01.000000");
        whenDeleteOccursForPK(pk3, "2023-11-13 10:00:02.000000");

        whenInsertOccursForPK(pk4, "data4a", "2023-11-13 10:00:01.000000");
        whenUpdateOccursForPK(pk4, "data4b", "2023-11-13 10:00:02.000000");

        whenInsertOccursForPK(pk5, "data5a", "2023-11-13 10:00:01.000000");
        whenUpdateOccursForPK(pk5, "data5b", "2023-11-13 10:00:02.000000");
        whenDeleteOccursForPK(pk5, "2023-11-13 10:00:03.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedDoNotContainPK(pk1);
        thenStructuredAndCuratedContainForPK("data2c", pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);
        thenStructuredAndCuratedContainForPK("data4b", pk4);
    }

    @Test
    public void shouldHandleInsertFollowedByUpdatesAndDeleteAcrossBatches() throws Exception {
        givenSourceReference();
        givenASourceReferenceSchema();
        givenASourceReferencePrimaryKey();
        givenAMatchingSchema();
        givenAnInputStream();
        givenTableStreamingQuery();
        givenTheStreamingQueryRuns();

        whenInsertOccursForPK(pk1, "data1", "2023-11-13 10:01:00.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data1", pk1);

        whenUpdateOccursForPK(pk1, "data2", "2023-11-13 10:02:00.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data2", pk1);

        whenUpdateOccursForPK(pk1, "data3", "2023-11-13 10:03:00.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data3", pk1);

        whenDeleteOccursForPK(pk1, "2023-11-13 10:04:00.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedDoNotContainPK(pk1);
    }

    @Test
    public void shouldHandleUpdateAndDeleteWithNoInsertFirst() throws Exception {
        givenSourceReference();
        givenASourceReferenceSchema();
        givenASourceReferencePrimaryKey();
        givenAMatchingSchema();
        givenAnInputStream();
        givenTableStreamingQuery();
        givenTheStreamingQueryRuns();

        whenUpdateOccursForPK(pk1, "data1", "2023-11-13 10:00:00.000000");
        whenDeleteOccursForPK(pk2, "2023-11-13 10:00:00.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data1", pk1);
        thenStructuredAndCuratedDoNotContainPK(pk2);
    }

    @Test
    public void shouldWriteNullsToViolationsForNonNullableColumns() throws Exception {
        givenSourceReference();
        givenASourceReferenceSchema();
        givenASourceReferencePrimaryKey();
        givenAMatchingSchema();
        givenAnInputStream();
        givenTableStreamingQuery();
        givenTheStreamingQueryRuns();

        whenInsertOccursForPK(pk1, "data1", "2023-11-13 10:01:00.000000");
        whenInsertOccursForPK(pk2, "data2", null);
        whenInsertOccursForPK(pk3, "data3", "2023-11-13 10:01:00.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data1", pk1);
        thenStructuredAndCuratedContainForPK("data3", pk3);

        thenStructuredAndCuratedDoNotContainPK(pk2);
        thenStructuredViolationsContainsForPK("data2", pk2);
    }

    @Test
    public void shouldWriteNoSchemaFoundToViolationsAcrossMultipleBatches() throws Exception {
        givenMissingSourceReference();
        givenAnInputStreamWithSchemaInference();
        givenTableStreamingQuery();
        givenTheStreamingQueryRuns();


        whenInsertOccursForPK(pk1, "data1", "2023-11-13 10:00:01.000000");
        whenUpdateOccursForPK(pk2, "data2", "2023-11-13 10:00:01.000000");
        whenDeleteOccursForPK(pk3, "2023-11-13 10:00:01.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredViolationsContainsForPK("data1", pk1);
        thenStructuredViolationsContainsForPK("data2", pk2);
        thenStructuredViolationsContainsForPK(null, pk3);
        thenStructuredAndCuratedDoNotContainPK(pk1);
        thenStructuredAndCuratedDoNotContainPK(pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);

        whenInsertOccursForPK(pk1, "data4", "2023-11-13 10:00:01.000000");
        whenUpdateOccursForPK(pk2, "data5", "2023-11-13 10:00:01.000000");
        whenDeleteOccursForPK(pk3, "2023-11-13 10:00:01.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredViolationsContainsForPK("data4", pk1);
        thenStructuredViolationsContainsForPK("data5", pk2);
        thenStructuredViolationsContainsForPK(null, pk3);
        thenStructuredAndCuratedDoNotContainPK(pk1);
        thenStructuredAndCuratedDoNotContainPK(pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);
    }

    @Test
    public void shouldWriteSchemaMismatchesToViolationsAcrossMultipleBatches() throws Exception {
        givenSourceReference();
        givenASourceReferenceSchema();
        givenASourceReferencePrimaryKey();
        givenASchemaMismatch();
        givenAnInputStream();
        givenTableStreamingQuery();
        givenTheStreamingQueryRuns();

        whenInsertOccursForPK(pk1, "data1", "2023-11-13 10:00:01.000000");
        whenUpdateOccursForPK(pk2, "data2", "2023-11-13 10:00:01.000000");
        whenDeleteOccursForPK(pk3, "2023-11-13 10:00:01.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredViolationsContainsForPK("data1", pk1);
        thenStructuredViolationsContainsForPK("data2", pk2);
        thenStructuredViolationsContainsForPK(null, pk3);
        thenStructuredAndCuratedDoNotContainPK(pk1);
        thenStructuredAndCuratedDoNotContainPK(pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);

        whenInsertOccursForPK(pk1, "data4", "2023-11-13 10:00:01.000000");
        whenUpdateOccursForPK(pk2, "data5", "2023-11-13 10:00:01.000000");
        whenDeleteOccursForPK(pk3, "2023-11-13 10:00:01.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredViolationsContainsForPK("data4", pk1);
        thenStructuredViolationsContainsForPK("data5", pk2);
        thenStructuredViolationsContainsForPK(null, pk3);
        thenStructuredAndCuratedDoNotContainPK(pk1);
        thenStructuredAndCuratedDoNotContainPK(pk2);
        thenStructuredAndCuratedDoNotContainPK(pk3);
    }

    private void givenAMatchingSchema() {
        when(dataProvider.inferSchema(any(), eq(inputSchemaName), eq(inputTableName)))
                .thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
    }

    private void givenASchemaMismatch() {
        StructType misMatchingSchema = SCHEMA_WITHOUT_METADATA_FIELDS.add(
                new StructField("an-extra-column", DataTypes.StringType, true, Metadata.empty())
        );
        when(dataProvider.inferSchema(any(), eq(inputSchemaName), eq(inputTableName)))
                .thenReturn(misMatchingSchema);
    }

    private void givenAnInputStream() {
        inputStream = new MemoryStream<Row>(1, spark.sqlContext(), Option.apply(10), encoder);
        Dataset<Row> streamingDataframe = inputStream.toDF();

        when(dataProvider.getStreamingSourceData(any(), eq(sourceReference))).thenReturn(streamingDataframe);
    }

    private void givenAnInputStreamWithSchemaInference() throws NoSchemaNoDataException {
        inputStream = new MemoryStream<Row>(1, spark.sqlContext(), Option.apply(10), encoder);
        Dataset<Row> streamingDataframe = inputStream.toDF();

        when(dataProvider.getStreamingSourceDataWithSchemaInference(any(), eq(inputSchemaName), eq(inputTableName))).thenReturn(streamingDataframe);
    }

    private void givenTheStreamingQueryRuns() {
        streamingQuery = underTest.runQuery();
    }

    private void givenPathsAreConfigured() {
        structuredPath = testRoot.resolve("structured").toAbsolutePath().toString();
        curatedPath = testRoot.resolve("curated").toAbsolutePath().toString();
        violationsPath = testRoot.resolve("violations").toAbsolutePath().toString();
        checkpointPath = testRoot.resolve("checkpoints").toAbsolutePath().toString();
        when(arguments.getStructuredS3Path()).thenReturn(structuredPath);
        when(arguments.getCuratedS3Path()).thenReturn(curatedPath);
        when(arguments.getViolationsS3Path()).thenReturn(violationsPath);
        when(arguments.getCheckpointLocation()).thenReturn(checkpointPath);
    }

    private void givenSourceReference() {
        when(sourceReferenceService.getSourceReference(inputSchemaName, inputTableName))
                .thenReturn(Optional.of(sourceReference));
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);
    }

    private void givenASourceReferenceSchema() {
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
    }

    private void givenASourceReferencePrimaryKey() {
        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey(PRIMARY_KEY_COLUMN));
    }
    private void givenMissingSourceReference() {
        when(sourceReferenceService.getSourceReference(inputSchemaName, inputTableName))
                .thenReturn(Optional.empty());
    }

    private void givenTableStreamingQuery() throws NoSchemaNoDataException {
        DataStorageService storageService = new DataStorageService(arguments);
        ViolationService violationService = new ViolationService(
                arguments,
                storageService,
                dataProvider,
                tableDiscoveryService
        );
        CdcBatchProcessor batchProcessor = new CdcBatchProcessor(
                new ValidationService(violationService),
                new StructuredZoneCDCS3(arguments, violationService, storageService),
                new CuratedZoneCDCS3(arguments, violationService, storageService),
                dataProvider
        );
        TableStreamingQueryProvider streamingQueryProvider = new TableStreamingQueryProvider(
                arguments,
                dataProvider,
                batchProcessor,
                sourceReferenceService,
                violationService
        );
        underTest = streamingQueryProvider.provide(spark, inputSchemaName, inputTableName);
    }

    private void whenTheNextBatchIsProcessed() {
        streamingQuery.processAllAvailable();
    }

    private void whenInsertOccursForPK(int primaryKey, String data, String timestamp) {
        List<Row> input = Collections.singletonList(
                createRow(primaryKey, timestamp, Insert, data)
        );
        whenDataIsAddedToTheInputStream(input);
    }

    private void whenUpdateOccursForPK(int primaryKey, String data, String timestamp) {
        List<Row> input = Collections.singletonList(
                createRow(primaryKey, timestamp, Update, data)
        );
        whenDataIsAddedToTheInputStream(input);
    }

    private void whenDeleteOccursForPK(int primaryKey, String timestamp) {
        List<Row> input = Collections.singletonList(
                createRow(primaryKey, timestamp, Delete, null)
        );
        whenDataIsAddedToTheInputStream(input);
    }

    private void whenDataIsAddedToTheInputStream(List<Row> inputData) {
        Seq<Row> input = convertListToSeq(inputData);
        inputStream.addData(input);
    }
}
