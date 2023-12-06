package uk.gov.justice.digital.client.s3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.NoSchemaNoDataException;
import uk.gov.justice.digital.test.BaseMinimalDataIntegrationTest;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;

@ExtendWith(MockitoExtension.class)
public class S3DataProviderNoFilesIT extends BaseMinimalDataIntegrationTest {
    private static final String sourceName = "source";
    private static final String tableName = "table";

    @TempDir
    protected Path testRootPath;

    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReference sourceReference;

    private S3DataProvider underTest;

    @BeforeEach
    public void setUp() {
        underTest = new S3DataProvider(arguments);
    }

    @Test
    public void getStreamingSourceDataShouldNotFailWhenTablePathExistsWithNoData() throws Exception {
        Path tablePath = testRootPath.resolve(sourceName).resolve(tableName).toAbsolutePath();
        tablePath.toFile().mkdirs();

        when(arguments.getRawS3Path()).thenReturn(testRootPath.toString());
        when(arguments.getCdcFileGlobPattern()).thenReturn(JobArguments.CDC_FILE_GLOB_PATTERN_DEFAULT);
        when(sourceReference.getSource()).thenReturn(sourceName);
        when(sourceReference.getTable()).thenReturn(tableName);
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);

        Dataset<Row> df = underTest.getStreamingSourceData(spark, sourceReference);

        StreamingQuery query = df.writeStream()
                .format("memory")
                .queryName("output")
                .outputMode(OutputMode.Append())
                .start();

        try {
            query.processAllAvailable();

        } finally {
            assertTrue(query.exception().isEmpty());
            query.stop();
        }
    }

    @Test
    public void getStreamingSourceDataShouldNotFailWhenRootPathExistsButTablePathDoesNot() throws Exception {
        testRootPath.toFile().mkdirs();

        when(arguments.getRawS3Path()).thenReturn(testRootPath.toString());
        when(arguments.getCdcFileGlobPattern()).thenReturn(JobArguments.CDC_FILE_GLOB_PATTERN_DEFAULT);
        when(sourceReference.getSource()).thenReturn(sourceName);
        when(sourceReference.getTable()).thenReturn(tableName);
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);

        Dataset<Row> df = underTest.getStreamingSourceData(spark, sourceReference);

        StreamingQuery query = df.writeStream()
                .format("memory")
                .queryName("output")
                .outputMode(OutputMode.Append())
                .start();

        try {
            query.processAllAvailable();

        } finally {
            assertTrue(query.exception().isEmpty());
            query.stop();
        }
    }

    @Test
    public void getStreamingSourceDataWithSchemaInferenceShouldThrowWhenTablePathExistsWithNoData() {
        Path tablePath = testRootPath.resolve(sourceName).resolve(tableName).toAbsolutePath();
        tablePath.toFile().mkdirs();

        when(arguments.getRawS3Path()).thenReturn(testRootPath.toString());
        when(arguments.getCdcFileGlobPattern()).thenReturn(JobArguments.CDC_FILE_GLOB_PATTERN_DEFAULT);
        assertThrows(NoSchemaNoDataException.class, () -> underTest.getStreamingSourceDataWithSchemaInference(spark, sourceName, tableName));
    }
}
