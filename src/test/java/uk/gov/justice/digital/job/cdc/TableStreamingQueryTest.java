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
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;

import java.io.IOException;
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
class TableStreamingQueryTest extends BaseSparkTest {

    private static final String inputSchemaName = "my-schema";
    private static final String inputTableName = "my-table";
    private static final String structuredPath = "/structured/path";
    private static final String curatedPath = "/curated/path";

    private static List<Row> testData;
    private static Seq<Row> testDataSeq;
    @Mock
    private JobArguments arguments;
    @Mock
    private S3DataProvider dataProvider;
    @Mock
    private CdcBatchProcessor batchProcessor;
    @Mock
    private SourceReference sourceReference;

    @TempDir
    private Path testRoot;

    private TableStreamingQuery underTest;

    private MemoryStream<Row> inputStream;

    @BeforeAll
    public static void setUpAll() {
        testData = rowPerPkDfSameTimestamp(spark).collectAsList();
        testDataSeq = convertListToSeq(testData);
    }

    @BeforeEach
    public void setUp() {
        underTest = new TableStreamingQuery(arguments, dataProvider, batchProcessor, inputSchemaName, inputTableName, sourceReference);

    }

    @Test
    public void runQueryShouldDelegateProcessingToBatchProcessor() throws IOException {
        givenASourceReference();
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

    private void thenProcessingIsDelegatedToBatchProcessor() {
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);
        verify(batchProcessor, times(1))
                .processBatch(
                        eq(sourceReference),
                        eq(spark),
                        argumentCaptor.capture(),
                        any(),
                        eq(structuredPath + "/my-schema/my-table/"),
                        eq(curatedPath + "/my-schema/my-table/")
                );
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(testData.size(), result.size());
        assertTrue(result.containsAll(testData));
    }

    private void givenAnInputStream() {
        inputStream = new MemoryStream<Row>(1, spark.sqlContext(), Option.apply(10), encoder);
        Dataset<Row> streamingDataframe = inputStream.toDF();

        when(dataProvider.getSourceData(any(), eq(inputSchemaName), eq(inputTableName))).thenReturn(streamingDataframe);
    }

    private void givenJobArguments() {
        String checkpointPath = testRoot.toAbsolutePath().toString();
        when(arguments.getStructuredS3Path()).thenReturn(structuredPath);
        when(arguments.getCuratedS3Path()).thenReturn(curatedPath);
        when(arguments.getCheckpointLocation()).thenReturn(checkpointPath);
    }

    private void givenASourceReference() {
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);
    }
}