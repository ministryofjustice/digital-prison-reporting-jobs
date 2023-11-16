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
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Option;
import scala.collection.Seq;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT;
import static uk.gov.justice.digital.test.MinimalTestData.DATA_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.encoder;
import static uk.gov.justice.digital.test.MinimalTestData.primaryKey;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;
import static uk.gov.justice.digital.test.SparkTestHelpers.convertListToSeq;

/**
 * This class tests TableStreamingQuery using mainly real dependencies, including integrating with real
 * delta lake operations on the local filesystem. This allows testing the processing logic together with the
 * delta lake merge operations to give a close to end-to-end set of tests for an individual table.
 * The input stream is mocked and uses Spark's MemoryStream. This allows us to simulate test scenarios where test data
 * can be received in a single batch or across multiple batches.
 */
@ExtendWith(MockitoExtension.class)
public class TableStreamingQueryIT extends BaseSparkTest {

    private static final int pk1 = 1;
    private static final int pk2 = 2;
    private static final int pk3 = 3;
    private static final int pk4 = 4;
    private static final int pk5 = 5;

    private static final String inputSchemaName = "my-schema";
    private static final String inputTableName = "my-table";
    @Mock
    private JobArguments arguments;
    @Mock
    private S3DataProvider dataProvider;
    @Mock
    private SourceReference sourceReference;


    private TableStreamingQuery underTest;

    private MemoryStream<Row> inputStream;
    private StreamingQuery streamingQuery;

    @TempDir
    private Path testRoot;
    private String structuredPath;
    private String curatedPath;
    private String violationsPath;
    private String checkpointPath;

    @BeforeEach
    public void setUp() {
        givenPathsAreConfigured();
        givenRetrySettingsAreConfigured();
        givenDependenciesAreInjected();

        givenASourceReference();
        givenAnInputStream();

        whenTheStreamingQueryRuns();
    }

    @AfterEach
    public void tearDown() throws TimeoutException {
        stopStreamingQuery();
    }

    @Test
    public void shouldHandleInsertsForMultiplePrimaryKeysInSameBatch() {

        whenInsertOccursForPK(pk1, "data1", "2023-11-13 10:00:01.000000");
        whenInsertOccursForPK(pk2, "data2", "2023-11-13 10:00:01.000000");
        whenInsertOccursForPK(pk3, "data3", "2023-11-13 10:00:02.000000");

        whenTheNextBatchIsProcessed();

        thenCuratedAndStructuredContainForPK("data1", pk1);
        thenCuratedAndStructuredContainForPK("data2", pk2);
        thenCuratedAndStructuredContainForPK("data3", pk3);
    }

    @Test
    public void shouldHandleMultiplePrimaryKeysAcrossBatches() {

        whenInsertOccursForPK(pk1, "data1a", "2023-11-13 10:00:01.000000");
        whenInsertOccursForPK(pk2, "data2a", "2023-11-13 10:00:01.000000");
        whenInsertOccursForPK(pk3, "data3a", "2023-11-13 10:00:02.000000");

        whenTheNextBatchIsProcessed();

        thenCuratedAndStructuredContainForPK("data1a", pk1);
        thenCuratedAndStructuredContainForPK("data2a", pk2);
        thenCuratedAndStructuredContainForPK("data3a", pk3);

        whenUpdateOccursForPK(pk1, "data1b", "2023-11-13 10:01:01.000000");
        whenUpdateOccursForPK(pk2, "data2b", "2023-11-13 10:01:01.000000");
        whenDeleteOccursForPK(pk3, "2023-11-13 10:01:01.000000");

        whenTheNextBatchIsProcessed();

        thenCuratedAndStructuredContainForPK("data1b", pk1);
        thenCuratedAndStructuredContainForPK("data2b", pk2);
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
        thenCuratedAndStructuredContainForPK("data2c", pk2);
        thenCuratedAndStructuredDoNotContainPK(pk3);
        thenCuratedAndStructuredContainForPK("data4b", pk4);
    }
    @Test
    public void shouldHandleInsertFollowedByUpdatesAndDeleteAcrossBatches() {
        whenInsertOccursForPK(pk1, "data1", "2023-11-13 10:01:00.000000");

        whenTheNextBatchIsProcessed();

        thenCuratedAndStructuredContainForPK("data1", pk1);

        whenUpdateOccursForPK(pk1, "data2", "2023-11-13 10:02:00.000000");

        whenTheNextBatchIsProcessed();

        thenCuratedAndStructuredContainForPK("data2", pk1);

        whenUpdateOccursForPK(pk1, "data3", "2023-11-13 10:03:00.000000");

        whenTheNextBatchIsProcessed();

        thenCuratedAndStructuredContainForPK("data3", pk1);

        whenDeleteOccursForPK(pk1, "2023-11-13 10:04:00.000000");

        whenTheNextBatchIsProcessed();

        thenCuratedAndStructuredDoNotContainPK(pk1);
    }

    @Test
    public void shouldHandleUpdateAndDeleteWithNoInsertFirst() {
        whenUpdateOccursForPK(pk1, "data1", "2023-11-13 10:00:00.000000");
        whenDeleteOccursForPK(pk2, "2023-11-13 10:00:00.000000");

        whenTheNextBatchIsProcessed();

        thenCuratedAndStructuredContainForPK("data1", pk1);
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

    private void givenRetrySettingsAreConfigured() {
        when(arguments.getDataStorageRetryMinWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT);
        when(arguments.getDataStorageRetryMaxWaitMillis()).thenReturn(DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT);
        when(arguments.getDataStorageRetryMaxAttempts()).thenReturn(DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT);
        when(arguments.getDataStorageRetryJitterFactor()).thenReturn(DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT);
    }

    private void givenDependenciesAreInjected() {
        // Manually creating dependencies because Micronaut test injection is not working
        DataStorageService storageService = new DataStorageService(arguments);
        ViolationService violationService = new ViolationService(arguments, storageService);
        ValidationService validationService = new ValidationService(violationService);
        CdcBatchProcessor batchProcessor = new CdcBatchProcessor(violationService, validationService, storageService);
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
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(sourceReference.getSchema()).thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
    }

    private void whenTheStreamingQueryRuns() {
        streamingQuery = underTest.runQuery(spark);
    }

    private void stopStreamingQuery() throws TimeoutException {
        streamingQuery.stop();
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

    private void thenCuratedAndStructuredContainForPK(String data, int primaryKey) {
        thenZoneContainsForPK(structuredPath, data, primaryKey);
        thenZoneContainsForPK(curatedPath, data, primaryKey);
    }

    private void thenZoneContainsForPK(String zonePath, String data, int primaryKey) {
        String tablePath = format("%s%s/%s",
                ensureEndsWithSlash(zonePath), sourceReference.getSource(), sourceReference.getTable()
        );

        Dataset<Row> df = spark.read().format("delta").load(tablePath);
        List<Row> result = df
                .select(DATA_COLUMN)
                .where(col(PRIMARY_KEY_COLUMN).equalTo(lit(Integer.toString(primaryKey))))
                .collectAsList();

        List<Row> expected = Collections.singletonList(RowFactory.create(data));
        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    private void thenCuratedAndStructuredDoNotContainPK(int primaryKey) {
        thenZoneDoesNotContainPK(structuredPath, primaryKey);
        thenZoneDoesNotContainPK(curatedPath, primaryKey);
    }

    private void thenZoneDoesNotContainPK(String zonePath, int primaryKey) {
        String tablePath = format("%s%s/%s",
                ensureEndsWithSlash(zonePath), sourceReference.getSource(), sourceReference.getTable()
        );

        Dataset<Row> df = spark.read().format("delta").load(tablePath);
        List<Row> result = df
                .select(DATA_COLUMN)
                .where(col(PRIMARY_KEY_COLUMN).equalTo(lit(Integer.toString(primaryKey))))
                .collectAsList();

        assertEquals(0, result.size());
    }
}
