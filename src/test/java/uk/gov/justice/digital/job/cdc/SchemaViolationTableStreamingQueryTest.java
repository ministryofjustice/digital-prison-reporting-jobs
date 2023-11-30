package uk.gov.justice.digital.job.cdc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Option;
import scala.collection.Seq;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.ViolationService;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.MinimalTestData.encoder;
import static uk.gov.justice.digital.test.MinimalTestData.rowPerPkDfSameTimestamp;
import static uk.gov.justice.digital.test.SparkTestHelpers.convertListToSeq;

@ExtendWith(MockitoExtension.class)
class SchemaViolationTableStreamingQueryTest extends BaseSparkTest {

    private static final String inputSourceName = "some-source";
    private static final String inputTableName = "some-table";

    private static List<Row> testData;
    private static Seq<Row> testDataSeq;

    @Mock
    private JobArguments arguments;
    @Mock
    private ViolationService violationService;
    @Mock
    private S3DataProvider dataProvider;

    @TempDir
    private Path testRoot;

    private SchemaViolationTableStreamingQuery underTest;

    private MemoryStream<Row> inputStream;

    @BeforeAll
    public static void setUpAll() {
        testData = rowPerPkDfSameTimestamp(spark).collectAsList();
        testDataSeq = convertListToSeq(testData);
    }

    @BeforeEach
    public void setUp() {
        underTest = new SchemaViolationTableStreamingQuery(
                inputSourceName,
                inputTableName,
                arguments,
                dataProvider,
                violationService
        );

    }

    @Test
    public void runQueryShouldDelegateProcessingToViolationsService() throws DataStorageException {
        givenJobArguments();
        givenAnInputStream();

        whenDataIsAddedToTheInputStream();
        whenTheStreamingQueryRuns();

        thenProcessingIsDelegatedToBatchProcessor();
    }

    private void whenTheStreamingQueryRuns() {
        StreamingQuery streamingQuery = underTest.runQuery(spark);
        streamingQuery.processAllAvailable();
    }

    private void whenDataIsAddedToTheInputStream() {
        inputStream.addData(testDataSeq);
    }

    private void thenProcessingIsDelegatedToBatchProcessor() throws DataStorageException {
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);
        verify(violationService, times(1))
                .handleNoSchemaFoundS3(
                        eq(spark),
                        argumentCaptor.capture(),
                        eq(inputSourceName),
                        eq(inputTableName)
                );
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(testData.size(), result.size());
        assertTrue(result.containsAll(testData));
    }

    private void givenAnInputStream() {
        inputStream = new MemoryStream<Row>(1, spark.sqlContext(), Option.apply(10), encoder);
        Dataset<Row> streamingDataframe = inputStream.toDF();

        when(dataProvider.getStreamingSourceDataWithSchemaInference(any(), eq(inputSourceName), eq(inputTableName))).thenReturn(streamingDataframe);
    }

    private void givenJobArguments() {
        String checkpointPath = testRoot.toAbsolutePath().toString();
        when(arguments.getCheckpointLocation()).thenReturn(checkpointPath);
    }
}