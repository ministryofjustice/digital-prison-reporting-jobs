package uk.gov.justice.digital.job;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.job.batchprocessing.S3BatchProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.SourceReferenceService;
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
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;

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
    private S3DataProvider dataProvider;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private SourceReference sourceReference1;
    @Mock
    private SourceReference sourceReference2;
    @Mock
    private SparkSession spark;
    @Mock
    private Dataset<Row> dataFrame;

    private DataHubBatchJob underTest;


    @BeforeEach
    public void setUp() {
        underTest = new DataHubBatchJob(arguments, properties, sparkSessionProvider, tableDiscoveryService, batchProcessor, dataProvider, sourceReferenceService);
    }

    @Test
    public void shouldRunAQueryPerTableButIgnoreTablesWithoutFiles() throws IOException {
        stubRawPath();
        stubReadData();
        stubDiscoveredTablePaths();
        stubSourceReference();

        underTest.runJob(spark);

        // Should process table 1 and table 2...
        verify(batchProcessor, times(1)).processBatch(any(), eq(sourceReference1), any());
        verify(batchProcessor, times(1)).processBatch(any(), eq(sourceReference2), any());
        // and no other tables...
        verify(batchProcessor, times(2)).processBatch(any(), any(), any());
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

    private void stubReadData() {
        when(dataProvider.getSourceDataBatch(any(), any(), any())).thenReturn(dataFrame);
        when(dataFrame.schema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
    }

    private void stubSourceReference() {
        when(sourceReferenceService.getSourceReferenceOrThrow(eq("s1"), eq("t1"))).thenReturn(sourceReference1);
        when(sourceReferenceService.getSourceReferenceOrThrow(eq("s2"), eq("t2"))).thenReturn(sourceReference2);
    }

}