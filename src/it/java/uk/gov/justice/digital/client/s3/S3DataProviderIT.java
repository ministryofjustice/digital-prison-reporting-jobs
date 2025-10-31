package uk.gov.justice.digital.client.s3;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.test.BaseMinimalDataIntegrationTest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.config.JobArguments.CDC_FILE_GLOB_PATTERN_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.inserts;

@ExtendWith(MockitoExtension.class)
class S3DataProviderIT extends BaseMinimalDataIntegrationTest {

    private static final String sourceName = "source";
    private static final String tableName = "table";
    private static final String processedRawFilesPath = "processed";
    @TempDir
    protected static Path testRootPath;
    protected static List<String> dataFilePaths;
    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private SourceReference.PrimaryKey primaryKey;
    @Mock
    SparkSession mockSparkSession;
    @Mock
    DataFrameReader mockDataFrameReader;
    @Mock
    Dataset<Row> mockRawDataset;
    @Mock
    Dataset<Row> mockArchivedDataset;
    @Mock
    Dataset<Row> mockProcessedDataset;

    private S3DataProvider underTest;

    @BeforeAll
    static void setUpAll() throws IOException {
        String tablePath = testRootPath.resolve(sourceName).resolve(tableName).toAbsolutePath().toString();
        Dataset<Row> testData = inserts(spark);
        testData.write().parquet(tablePath);
        FileSystem fs = FileSystem.get(URI.create(tablePath), spark.sparkContext().hadoopConfiguration());
        FileStatus[] fileStatuses = fs.listStatus(new org.apache.hadoop.fs.Path(tablePath));
        dataFilePaths = Arrays.stream(fileStatuses)
                .filter(FileStatus::isFile)
                .map(f -> f.getPath().toString())
                .toList();
    }

    @BeforeEach
    void setUp() {
        underTest = new S3DataProvider(arguments);
    }

    @Test
    void shouldGetBatchSourceDataWithSchemaInference() {
        Dataset<Row> df = underTest.getBatchSourceData(spark, dataFilePaths);
        assertEquals(3, df.count());
    }

    @Test
    void shouldGetPrimaryKeysFromCuratedZone() {
        when(arguments.getCuratedS3Path()).thenReturn(testRootPath.toString());
        when(sourceReference.getSource()).thenReturn(sourceName);
        when(sourceReference.getTable()).thenReturn(tableName);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getSparkKeyColumns()).thenReturn(
                JavaConverters.asScalaBufferConverter(Collections.singletonList(new Column(PRIMARY_KEY_COLUMN))).asScala().toSeq()
        );

        Dataset<Row> df = underTest.getPrimaryKeysInCurated(spark, sourceReference);
        assertEquals(3, df.count());
        assertArrayEquals(new String[] {PRIMARY_KEY_COLUMN}, df.columns());
    }

    @Test
    void shouldGetStreamingSourceDataWithSchemaInference() throws Exception {
        when(arguments.getRawS3Path()).thenReturn(testRootPath.toString());
        when(arguments.enableStreamingSourceArchiving()).thenReturn(false);
        when(arguments.getCdcFileGlobPattern()).thenReturn(CDC_FILE_GLOB_PATTERN_DEFAULT);
        when(arguments.streamingJobMaxFilePerTrigger()).thenReturn(STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER);

        Dataset<Row> df = underTest.getStreamingSourceDataWithSchemaInference(spark, sourceName, tableName);
        StreamingQuery query = df.writeStream()
                .format("memory")
                .queryName("output")
                .outputMode(OutputMode.Append())
                .start();

        assertOnQuery(query);
    }

    @Test
    void shouldGetStreamingSourceDataWithSpecifiedSchema() throws Exception {
        when(arguments.getRawS3Path()).thenReturn(testRootPath.toString());
        when(arguments.enableStreamingSourceArchiving()).thenReturn(false);
        when(arguments.getCdcFileGlobPattern()).thenReturn(CDC_FILE_GLOB_PATTERN_DEFAULT);
        when(arguments.streamingJobMaxFilePerTrigger()).thenReturn(STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER);
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getSource()).thenReturn(sourceName);
        when(sourceReference.getTable()).thenReturn(tableName);

        Dataset<Row> df = underTest.getStreamingSourceData(spark, sourceReference);

        StreamingQuery query = df.writeStream()
                .format("memory")
                .queryName("output")
                .outputMode(OutputMode.Append())
                .start();

        assertOnQuery(query);
    }

    @Test
    void getStreamingSourceDataWithSchemaInferenceShouldConfigureArchivingOfProcessedFilesWhenEnabled() throws Exception {
        when(arguments.getRawS3Path()).thenReturn(testRootPath.toString());
        when(arguments.enableStreamingSourceArchiving()).thenReturn(true);
        when(arguments.getProcessedRawFilesPath()).thenReturn(processedRawFilesPath);
        when(arguments.getCdcFileGlobPattern()).thenReturn(CDC_FILE_GLOB_PATTERN_DEFAULT);
        when(arguments.streamingJobMaxFilePerTrigger()).thenReturn(STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER);

        Dataset<Row> df = underTest.getStreamingSourceDataWithSchemaInference(spark, sourceName, tableName);

        StreamingQuery query = df.writeStream()
                .format("memory")
                .queryName("output")
                .outputMode(OutputMode.Append())
                .start();

        assertOnQuery(query);
    }

    @Test
    void getStreamingSourceDataShouldConfigureArchivingOfProcessedFilesWhenEnabled() throws TimeoutException {
        when(arguments.getRawS3Path()).thenReturn(testRootPath.toString());
        when(arguments.enableStreamingSourceArchiving()).thenReturn(true);
        when(arguments.getProcessedRawFilesPath()).thenReturn(processedRawFilesPath);
        when(arguments.getCdcFileGlobPattern()).thenReturn(CDC_FILE_GLOB_PATTERN_DEFAULT);
        when(arguments.streamingJobMaxFilePerTrigger()).thenReturn(STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER);
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getSource()).thenReturn(sourceName);
        when(sourceReference.getTable()).thenReturn(tableName);

        Dataset<Row> df = underTest.getStreamingSourceData(spark, sourceReference);

        StreamingQuery query = df.writeStream()
                .format("memory")
                .queryName("output")
                .outputMode(OutputMode.Append())
                .start();

        assertOnQuery(query);
    }

    @Test
    void shouldInferSchema() {
        when(arguments.getRawS3Path()).thenReturn(testRootPath.toString());
        StructType inferredSchema = underTest.inferSchema(spark, sourceName, tableName);
        assertEquals(TEST_DATA_SCHEMA, inferredSchema);
    }

    @Test
    void shouldInferSchemaFromArchiveWhenSchemaInferenceFromRawPathThrowsFileNotFoundException() {
        String rawPath = "/raw-path";
        String rawArchivePath = "/raw-archive-path";

        when(arguments.getRawS3Path()).thenReturn(rawPath);
        when(arguments.getRawArchiveS3Path()).thenReturn(rawArchivePath);

        when(mockSparkSession.read()).thenReturn(mockDataFrameReader);

        when(mockDataFrameReader.parquet(String.format("%s/%s/%s", rawPath, sourceName, tableName)))
                .thenReturn(mockRawDataset);
        when(mockRawDataset.schema())
                .thenThrow(new RuntimeException(new SparkException("some spark exception", new FileNotFoundException())));
        when(mockDataFrameReader.parquet(String.format("%s/%s/%s", rawArchivePath, sourceName, tableName)))
                .thenReturn(mockArchivedDataset);
        when(mockArchivedDataset.schema()).thenReturn(TEST_DATA_SCHEMA);

        StructType inferredSchema = underTest.inferSchema(mockSparkSession, sourceName, tableName);

        assertEquals(TEST_DATA_SCHEMA, inferredSchema);
    }

    @Test
    void shouldThrowExceptionWhenSchemaInferenceThrowsAnotherExceptionInsteadOfFileNotFoundException() {
        String rawPath = "/raw-path";

        when(arguments.getRawS3Path()).thenReturn(rawPath);
        when(mockSparkSession.read()).thenReturn(mockDataFrameReader);
        when(mockDataFrameReader.parquet(String.format("%s/%s/%s", rawPath, sourceName, tableName)))
                .thenReturn(mockRawDataset);
        when(mockRawDataset.schema())
                .thenThrow(new RuntimeException(new SparkException("some spark exception", new Exception("not a file-not-found-exception"))));

        assertThrows(Exception.class, () -> underTest.inferSchema(mockSparkSession, sourceName, tableName));
    }

    @Test
    void shouldInferSchemaFromProcessedFilesPathWhenStreamingSourceArchiveIsEnabledAndSchemaInferenceFromRawPathThrowsFileNotFoundException() {
        String rawPath = "/raw-path";
        String processedFilesFolder = "processed-files-folder";
        String rawArchivePath = "/raw-archive-path";

        when(arguments.enableStreamingSourceArchiving()).thenReturn(true);
        when(arguments.getRawS3Path()).thenReturn(rawPath);
        when(arguments.getProcessedRawFilesPath()).thenReturn(processedFilesFolder);
        when(arguments.getRawArchiveS3Path()).thenReturn(rawArchivePath);

        when(mockSparkSession.read()).thenReturn(mockDataFrameReader);

        when(mockDataFrameReader.parquet(String.format("%s/%s/%s", rawPath, sourceName, tableName)))
                .thenReturn(mockRawDataset);
        when(mockDataFrameReader.parquet(String.format("%s/%s/%s/%s", rawPath, processedFilesFolder, sourceName, tableName)))
                .thenReturn(mockProcessedDataset);
        when(mockRawDataset.schema())
                .thenThrow(new RuntimeException(new SparkException("some spark exception", new FileNotFoundException())));
        when(mockProcessedDataset.schema()).thenReturn(TEST_DATA_SCHEMA);

        StructType inferredSchema = underTest.inferSchema(mockSparkSession, sourceName, tableName);

        assertEquals(TEST_DATA_SCHEMA, inferredSchema);
    }

    @Test
    void shouldInferSchemaFromArchivePathWhenStreamingSourceArchiveIsEnabledAndSchemaInferenceFromProcessedFilesPathThrowsFileNotFoundException() {
        String rawPath = "/raw-path";
        String processedFilesFolder = "processed-files-folder";
        String rawArchivePath = "/raw-archive-path";

        when(arguments.enableStreamingSourceArchiving()).thenReturn(true);
        when(arguments.getRawS3Path()).thenReturn(rawPath);
        when(arguments.getProcessedRawFilesPath()).thenReturn(processedFilesFolder);
        when(arguments.getRawArchiveS3Path()).thenReturn(rawArchivePath);

        when(mockSparkSession.read()).thenReturn(mockDataFrameReader);

        when(mockRawDataset.schema())
                .thenThrow(new RuntimeException(new SparkException("some spark exception", new FileNotFoundException())));
        when(mockDataFrameReader.parquet(String.format("%s/%s/%s", rawPath, sourceName, tableName)))
                .thenReturn(mockRawDataset);
        when(mockDataFrameReader.parquet(String.format("%s/%s/%s/%s", rawPath, processedFilesFolder, sourceName, tableName)))
                .thenReturn(mockProcessedDataset);
        when(mockProcessedDataset.schema())
                .thenThrow(new RuntimeException(new SparkException("some spark exception", new FileNotFoundException())));
        when(mockDataFrameReader.parquet(String.format("%s/%s/%s", rawArchivePath, sourceName, tableName)))
                .thenReturn(mockArchivedDataset);
        when(mockArchivedDataset.schema()).thenReturn(TEST_DATA_SCHEMA);

        StructType inferredSchema = underTest.inferSchema(mockSparkSession, sourceName, tableName);

        assertEquals(TEST_DATA_SCHEMA, inferredSchema);
    }

    private static void assertOnQuery(StreamingQuery query) throws TimeoutException {
        try {
            query.processAllAvailable();
            Dataset<Row> result = spark.sql("select * from output");
            assertEquals(3, result.count());
        } finally {
            query.stop();
        }
    }
}
