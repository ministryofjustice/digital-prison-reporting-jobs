package uk.gov.justice.digital.job;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.collection.Seq;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.job.batchprocessing.S3BatchProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.TableDiscoveryService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;

@ExtendWith(MockitoExtension.class)
class DataHubBatchJobTest {
    private static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
    private static final String rawPath = "s3://raw/path";
    private static final Map<ImmutablePair<String, String>, List<String>> discoveredPathsByTable;

    static {
        discoveredPathsByTable = new HashMap<>();
        discoveredPathsByTable.put(new ImmutablePair<>("s1", "t1"), Collections.singletonList(
                "t1-file1"
        ));
        discoveredPathsByTable.put(new ImmutablePair<>("s2", "t2"), Arrays.asList(
                "t2-file1", "t2-file2"
        ));
        discoveredPathsByTable.put(new ImmutablePair<>("s3", "t3"), Collections.emptyList());
    }
    @Mock
    private JobArguments arguments;
    @Mock
    private JobProperties properties;
    @Mock
    private TableDiscoveryService tableDiscoveryService;
    @Mock
    private S3BatchProcessor batchProcessor;
    @Mock
    private SparkSession spark;
    @Mock
    private DataFrameReader dataFrameReader;
    @Mock
    private Dataset<Row> dataFrame;

    private DataHubBatchJob underTest;


    @BeforeEach
    public void setUp() {
        underTest = new DataHubBatchJob(arguments, properties, sparkSessionProvider, tableDiscoveryService, batchProcessor);
    }

    @Test
    public void shouldRunAQueryPerTableButIgnoreTablesWithoutFiles() throws IOException {
        stubRawPath();
        stubDataframeRead();
        stubDiscoveredTablePaths();

        underTest.runJob(spark);

        verify(batchProcessor, times(1)).processBatch(any(), eq("s1"), eq("t1"), any());
        verify(batchProcessor, times(1)).processBatch(any(), eq("s2"), eq("t2"), any());
        verify(batchProcessor, times(0)).processBatch(any(), eq("s3"), eq("t3"), any());
    }

    @Test
    public void shouldThrowForNoTables() throws IOException {
        stubRawPath();
        stubEmptyDiscoveredTablePaths();
        assertThrows(RuntimeException.class, () -> underTest.runJob(spark));
    }

    private void stubRawPath() {
        when(arguments.getRawS3Path()).thenReturn(rawPath);
    }

    private void stubDiscoveredTablePaths() throws IOException {
        when(tableDiscoveryService.discoverBatchFilesToLoad(rawPath, spark)).thenReturn(discoveredPathsByTable);
    }

    private void stubEmptyDiscoveredTablePaths() throws IOException {
        when(tableDiscoveryService.discoverBatchFilesToLoad(rawPath, spark)).thenReturn(Collections.emptyMap());
    }

    private void stubDataframeRead() {
        when(spark.read()).thenReturn(dataFrameReader);
        when(dataFrameReader.parquet(any(Seq.class))).thenReturn(dataFrame);
        when(dataFrame.schema()).thenReturn(TEST_DATA_SCHEMA);
    }

}