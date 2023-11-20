package uk.gov.justice.digital.job.cdc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
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
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.test.BaseMinimalDataIntegrationTest;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDCS3;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDCS3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
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
    private SourceReference sourceReference;

    private TableStreamingQuery underTest;

    private MemoryStream<Row> inputStream;
    private StreamingQuery streamingQuery;



    @BeforeEach
    public void setUp() {
        givenPathsAreConfigured();
        givenRetrySettingsAreConfigured(arguments);
        givenTableStreamingQueryDependenciesAreInjected();
        givenASourceReference();

        givenAnInputStream();
        givenTheStreamingQueryRuns();
    }

    @AfterEach
    public void tearDown() throws TimeoutException {
        streamingQuery.stop();
    }

    @Test
    public void shouldHandleInsertsForMultiplePrimaryKeysInSameBatch() {

        whenInsertOccursForPK(pk1, "data1", "2023-11-13 10:00:01.000000");
        whenInsertOccursForPK(pk2, "data2", "2023-11-13 10:00:01.000000");
        whenInsertOccursForPK(pk3, "data3", "2023-11-13 10:00:02.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data1", pk1);
        thenStructuredAndCuratedContainForPK("data2", pk2);
        thenStructuredAndCuratedContainForPK("data3", pk3);
    }

    @Test
    public void shouldHandleMultiplePrimaryKeysAcrossBatches() {

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
        thenCuratedAndStructuredDoNotContainPK(pk3);
    }

    @Test
    public void shouldHandleInsertFollowedByUpdatesAndDeleteInSameBatchWithDifferentTimestamps() {
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

        thenCuratedAndStructuredDoNotContainPK(pk1);
        thenStructuredAndCuratedContainForPK("data2c", pk2);
        thenCuratedAndStructuredDoNotContainPK(pk3);
        thenStructuredAndCuratedContainForPK("data4b", pk4);
    }
    @Test
    public void shouldHandleInsertFollowedByUpdatesAndDeleteAcrossBatches() {
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

        thenCuratedAndStructuredDoNotContainPK(pk1);
    }

    @Test
    public void shouldHandleUpdateAndDeleteWithNoInsertFirst() {
        whenUpdateOccursForPK(pk1, "data1", "2023-11-13 10:00:00.000000");
        whenDeleteOccursForPK(pk2, "2023-11-13 10:00:00.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data1", pk1);
        thenCuratedAndStructuredDoNotContainPK(pk2);

    }

    @Test
    public void shouldWriteNullsToViolationsForNonNullableColumns() {
        whenInsertOccursForPK(pk1, "data1", "2023-11-13 10:01:00.000000");
        whenInsertOccursForPK(pk2, "data2", null);
        whenInsertOccursForPK(pk3, "data3", "2023-11-13 10:01:00.000000");

        whenTheNextBatchIsProcessed();

        thenStructuredAndCuratedContainForPK("data1", pk1);
        thenStructuredAndCuratedContainForPK("data3", pk3);

        thenViolationsContainsForPK("data2", pk2);
        thenCuratedAndStructuredDoNotContainPK(pk2);
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

    private void givenTableStreamingQueryDependenciesAreInjected() {
        DataStorageService storageService = new DataStorageService(arguments);
        ViolationService violationService = new ViolationService(arguments, storageService);
        ValidationService validationService = new ValidationService(violationService);
        CuratedZoneCDCS3 curatedZone = new CuratedZoneCDCS3(arguments, violationService, storageService);
        StructuredZoneCDCS3 structuredZone = new StructuredZoneCDCS3(arguments, violationService, storageService);
        CdcBatchProcessor batchProcessor = new CdcBatchProcessor(validationService, structuredZone, curatedZone);
        underTest = new TableStreamingQuery(arguments, dataProvider, batchProcessor, inputSchemaName, inputTableName, sourceReference);
    }

    private void givenAnInputStream() {
        inputStream = new MemoryStream<Row>(1, spark.sqlContext(), Option.apply(10), encoder);
        Dataset<Row> streamingDataframe = inputStream.toDF();

        when(dataProvider.getSourceData(any(), eq(inputSchemaName), eq(inputTableName))).thenReturn(streamingDataframe);
    }

    private void givenASourceReference() {
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(sourceReference.getSchema()).thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
    }

    private void givenTheStreamingQueryRuns() {
        streamingQuery = underTest.runQuery(spark);
    }

    private void whenTheNextBatchIsProcessed() {
        streamingQuery.processAllAvailable();
    }

    private void whenInsertOccursForPK(int primaryKey, String data, String timestamp) {
        List<Row> input = Collections.singletonList(
                RowFactory.create(Integer.toString(primaryKey), timestamp, "I", data)
        );
        whenDataIsAddedToTheInputStream(input);
    }

    private void whenUpdateOccursForPK(int primaryKey, String data, String timestamp) {
        List<Row> input = Collections.singletonList(
                RowFactory.create(Integer.toString(primaryKey), timestamp, "U", data)
        );
        whenDataIsAddedToTheInputStream(input);
    }

    private void whenDeleteOccursForPK(int primaryKey, String timestamp) {
        List<Row> input = Collections.singletonList(
                RowFactory.create(Integer.toString(primaryKey), timestamp, "D", null)
        );
        whenDataIsAddedToTheInputStream(input);
    }

    private void whenDataIsAddedToTheInputStream(List<Row> inputData) {
        Seq<Row> input = convertListToSeq(inputData);
        inputStream.addData(input);
    }
}
