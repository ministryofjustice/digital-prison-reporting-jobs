package uk.gov.justice.digital.job.cdc;

import org.apache.spark.api.java.function.VoidFunction2;
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
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Option;
import scala.collection.Seq;
import uk.gov.justice.digital.config.BaseSparkTest;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.gov.justice.digital.test.MinimalTestData.encoder;
import static uk.gov.justice.digital.test.MinimalTestData.rowPerPkDfSameTimestamp;
import static uk.gov.justice.digital.test.SparkTestHelpers.convertListToSeq;

@ExtendWith(MockitoExtension.class)
class TableStreamingQueryTest extends BaseSparkTest {
    private static final String source = "some-source";
    private static final String table = "some-table";
    private static List<Row> testData;
    private static Seq<Row> testDataSeq;
    @Mock
    private VoidFunction2<Dataset<Row>, Long> batchProcessingFunc;
    @TempDir
    private Path testRoot;
    @Captor
    private ArgumentCaptor<Dataset<Row>> argumentCaptor;

    private TableStreamingQuery underTest;

    private MemoryStream<Row> inputStream;

    @BeforeAll
    public static void setUpAll() {
        testData = rowPerPkDfSameTimestamp(spark).collectAsList();
        testDataSeq = convertListToSeq(testData);
    }

    @BeforeEach
    public void setUp() {
        String checkpointPath = testRoot.toAbsolutePath().toString();
        inputStream = new MemoryStream<Row>(1, spark.sqlContext(), Option.apply(10), encoder);
        Dataset<Row> streamingDataframe = inputStream.toDF();
        underTest = new TableStreamingQuery(
                source,
                table,
                checkpointPath,
                streamingDataframe,
                batchProcessingFunc
        );
    }

    @Test
    public void shouldDelegateToBatchProcessingFunc() throws Exception {
        inputStream.addData(testDataSeq);
        StreamingQuery sparkStreamingQuery = underTest.runQuery();
        sparkStreamingQuery.processAllAvailable();

        verify(batchProcessingFunc, times(1))
                .call(argumentCaptor.capture(), any());
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(testData.size(), result.size());
        assertTrue(result.containsAll(testData));
    }
}