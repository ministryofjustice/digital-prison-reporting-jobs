package uk.gov.justice.digital.client.s3;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.test.BaseMinimalDataIntegrationTest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.config.JobArguments.CDC_FILE_GLOB_PATTERN_DEFAULT;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;
import static uk.gov.justice.digital.test.MinimalTestData.inserts;

@ExtendWith(MockitoExtension.class)
public class S3DataProviderIT extends BaseMinimalDataIntegrationTest {

    private static final String sourceName = "source";
    private static final String tableName = "table";
    @TempDir
    protected static Path testRootPath;
    protected static List<String> dataFilePaths;
    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReference sourceReference;

    private S3DataProvider underTest;

    @BeforeAll
    public static void setUpAll() throws IOException {
        String tablePath = testRootPath.resolve(sourceName).resolve(tableName).toAbsolutePath().toString();
        Dataset<Row> testData = inserts(spark);
        testData.write().parquet(tablePath);
        FileSystem fs = FileSystem.get(URI.create(tablePath), spark.sparkContext().hadoopConfiguration());
        FileStatus[] fileStatuses = fs.listStatus(new org.apache.hadoop.fs.Path(tablePath));
        dataFilePaths = Arrays.stream(fileStatuses)
                .filter(FileStatus::isFile)
                .map(f -> f.getPath().toString())
                .collect(Collectors.toList());
    }

    @BeforeEach
    public void setUp() {
        underTest = new S3DataProvider(arguments);
    }

    @Test
    public void shouldGetBatchSourceDataWithSchemaInference() {
        Dataset<Row> df = underTest.getBatchSourceDataWithSchemaInference(spark, dataFilePaths);
        assertEquals(3, df.count());
    }

    @Test
    public void shouldGetBatchSourceDataWithSpecifiedSchema() {
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getSource()).thenReturn(sourceName);
        when(sourceReference.getTable()).thenReturn(tableName);

        Dataset<Row> df = underTest.getBatchSourceData(spark, sourceReference, dataFilePaths);
        assertEquals(3, df.count());
    }

    @Test
    public void shouldGetStreamingSourceDataWithSchemaInference() {
        when(arguments.getRawS3Path()).thenReturn(testRootPath.toString());
        when(arguments.getCdcFileGlobPattern()).thenReturn(CDC_FILE_GLOB_PATTERN_DEFAULT);

        underTest.getStreamingSourceDataWithSchemaInference(spark, sourceName, tableName);
    }

    @Test
    public void shouldGetStreamingSourceDataWithSpecifiedSchema() {
        when(arguments.getRawS3Path()).thenReturn(testRootPath.toString());
        when(arguments.getCdcFileGlobPattern()).thenReturn(CDC_FILE_GLOB_PATTERN_DEFAULT);
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getSource()).thenReturn(sourceName);
        when(sourceReference.getTable()).thenReturn(tableName);

        underTest.getStreamingSourceData(spark, sourceReference);
    }
}
