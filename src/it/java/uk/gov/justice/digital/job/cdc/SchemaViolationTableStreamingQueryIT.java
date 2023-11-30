package uk.gov.justice.digital.job.cdc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.test.BaseMinimalDataIntegrationTest;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.MinimalTestData.encoder;
import static uk.gov.justice.digital.test.SparkTestHelpers.convertListToSeq;

@ExtendWith(MockitoExtension.class)
public class SchemaViolationTableStreamingQueryIT extends BaseMinimalDataIntegrationTest {

    @Mock
    private JobArguments arguments;
    @Mock
    private S3DataProvider dataProvider;

    private SchemaViolationTableStreamingQuery underTest;

    private MemoryStream<Row> inputStream;

    private StreamingQuery streamingQuery;

    @BeforeEach
    public void setUp() {
        givenPathsAreConfigured();
        givenRetrySettingsAreConfigured(arguments);
        givenTableStreamingQueryDependenciesAreInjected();
        givenAnInputStream();
        givenTheStreamingQueryRuns();
    }

    @AfterEach
    public void tearDown() throws TimeoutException {
        streamingQuery.stop();
    }

    @Test
    public void shouldWriteToViolationsAcrossMultipleBatches() {
        whenInsertOccursForPK(pk1, "data1", "2023-11-13 10:00:01.000000");
        whenUpdateOccursForPK(pk2, "data2", "2023-11-13 10:00:01.000000");
        whenDeleteOccursForPK(pk3, "2023-11-13 10:00:01.000000");

        whenTheNextBatchIsProcessed();

        thenViolationsContainsForPK("data1", pk1);
        thenViolationsContainsForPK("data2", pk2);
        thenViolationsContainsForPK(null, pk3);
        thenCuratedAndStructuredDoNotContainPK(pk1);
        thenCuratedAndStructuredDoNotContainPK(pk2);
        thenCuratedAndStructuredDoNotContainPK(pk3);

        whenInsertOccursForPK(pk1, "data4", "2023-11-13 10:00:01.000000");
        whenUpdateOccursForPK(pk2, "data5", "2023-11-13 10:00:01.000000");
        whenDeleteOccursForPK(pk3, "2023-11-13 10:00:01.000000");

        whenTheNextBatchIsProcessed();

        thenViolationsContainsForPK("data4", pk1);
        thenViolationsContainsForPK("data5", pk2);
        thenViolationsContainsForPK(null, pk3);
        thenCuratedAndStructuredDoNotContainPK(pk1);
        thenCuratedAndStructuredDoNotContainPK(pk2);
        thenCuratedAndStructuredDoNotContainPK(pk3);


    }

    private void givenAnInputStream() {
        inputStream = new MemoryStream<Row>(1, spark.sqlContext(), Option.apply(10), encoder);
        Dataset<Row> streamingDataframe = inputStream.toDF();

        when(dataProvider.getStreamingSourceDataWithSchemaInference(any(), eq(inputSchemaName), eq(inputTableName))).thenReturn(streamingDataframe);
    }

    private void givenTheStreamingQueryRuns() {
        streamingQuery = underTest.runQuery(spark);
    }

    private void givenPathsAreConfigured() {
        structuredPath = testRoot.resolve("structured").toAbsolutePath().toString();
        curatedPath = testRoot.resolve("curated").toAbsolutePath().toString();
        violationsPath = testRoot.resolve("violations").toAbsolutePath().toString();
        checkpointPath = testRoot.resolve("checkpoints").toAbsolutePath().toString();
        when(arguments.getViolationsS3Path()).thenReturn(violationsPath);
        when(arguments.getCheckpointLocation()).thenReturn(checkpointPath);
    }

    private void givenTableStreamingQueryDependenciesAreInjected() {
        underTest = new SchemaViolationTableStreamingQuery(
                inputSchemaName,
                inputTableName,
                arguments,
                dataProvider,
                new ViolationService(arguments, new DataStorageService(arguments))
        );
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
