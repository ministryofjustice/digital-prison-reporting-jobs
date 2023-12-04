package uk.gov.justice.digital.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.RAW;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;
import static uk.gov.justice.digital.test.MinimalTestData.inserts;

@ExtendWith(MockitoExtension.class)
class ViolationServiceTest extends BaseSparkTest {

    private static final String violationsPath = "s3://some-path";

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private DataStorageService mockDataStorage;
    @Mock
    private Dataset<Row> mockDataFrame;
    @Mock
    private DataStorageRetriesExhaustedException mockCause;

    private ViolationService underTest;

    @BeforeEach
    public void setUp() {
        when(mockJobArguments.getViolationsS3Path()).thenReturn(violationsPath);
        underTest = new ViolationService(mockJobArguments, mockDataStorage);
    }

    @Test
    public void handleRetriesExhaustedShouldWriteViolations() throws DataStorageException {
        underTest.handleRetriesExhausted(spark, testInputDataframe(), "source", "table", mockCause, RAW);
        verify(mockDataStorage).append(any(), any());
        verify(mockDataStorage).updateDeltaManifestForTable(any(), any());
    }

    @Test
    public void handleRetriesExhaustedShouldThrowIfWriteFails() throws DataStorageException {
        doThrow(DataStorageException.class).when(mockDataStorage).append(any(), any());
        assertThrows(RuntimeException.class, () -> underTest.handleRetriesExhausted(spark, testInputDataframe(), "source", "table", mockCause, RAW));
    }

    @Test
    public void handleRetriesExhaustedS3ShouldWriteViolations() throws DataStorageException {
        Dataset<Row> inputDf = inserts(spark);
        underTest.handleRetriesExhaustedS3(spark, inputDf, "source", "table", mockCause, STRUCTURED_LOAD);
        verify(mockDataStorage).append(eq("s3://some-path/structured/source/table"), any());
        verify(mockDataStorage).updateDeltaManifestForTable(any(), any());
    }

    @Test
    public void handleRetriesExhaustedS3ShouldThrowIfWriteFails() throws DataStorageException {
        Dataset<Row> inputDf = inserts(spark);
        doThrow(DataStorageException.class).when(mockDataStorage).append(any(), any());
        assertThrows(RuntimeException.class, () -> underTest.handleRetriesExhaustedS3(spark, inputDf, "source", "table", mockCause, RAW));
    }

    @Test
    public void handleNoSchemaFoundShouldWriteViolations() throws DataStorageException {
        underTest.handleNoSchemaFound(spark, testInputDataframe(), "source", "table");
        verify(mockDataStorage).append(any(), any());
        verify(mockDataStorage).updateDeltaManifestForTable(any(), any());
    }

    @Test
    public void handleNoSchemaFoundShouldThrowIfWriteFails() throws DataStorageException {
        doThrow(DataStorageException.class).when(mockDataStorage).append(any(), any());
        assertThrows(DataStorageException.class, () -> underTest.handleNoSchemaFound(spark, testInputDataframe(), "source", "table"));
    }

    @Test
    public void handleNoSchemaS3FoundShouldWriteViolations() throws DataStorageException {
        underTest.handleNoSchemaFoundS3(spark, testInputDataframe(), "source", "table", STRUCTURED_LOAD);
        verify(mockDataStorage).append(eq("s3://some-path/structured/source/table"), any());
        verify(mockDataStorage).updateDeltaManifestForTable(any(), any());
    }

    @Test
    public void handleNoSchemaS3FoundShouldThrowIfWriteFails() throws DataStorageException {
        doThrow(DataStorageException.class).when(mockDataStorage).append(any(), any());
        assertThrows(DataStorageException.class, () -> underTest.handleNoSchemaFoundS3(spark, testInputDataframe(), "source", "table", STRUCTURED_LOAD));
    }

    @Test
    public void handleInvalidSchemaShouldWriteViolations() throws DataStorageException {
        underTest.handleInvalidSchema(spark, testInputDataframe(), "source", "table", STRUCTURED_LOAD);
        verify(mockDataStorage).append(eq("s3://some-path/structured/source/table"), any());
        verify(mockDataStorage).updateDeltaManifestForTable(any(), any());
    }

    @Test
    public void handleInvalidSchemaShouldThrowIfWriteFails() throws DataStorageException {
        doThrow(DataStorageException.class).when(mockDataStorage).append(any(), any());
        assertThrows(DataStorageException.class, () -> underTest.handleInvalidSchema(spark, testInputDataframe(), "source", "table", STRUCTURED_LOAD));
    }

    @Test
    public void handleViolationShouldWriteViolations() throws DataStorageException {
        underTest.handleViolation(spark, testInputDataframe(), "source", "table", STRUCTURED_LOAD);
        verify(mockDataStorage).append(eq("s3://some-path/structured/source/table"), any());
        verify(mockDataStorage).updateDeltaManifestForTable(any(), any());
    }

    @Test
    public void handleViolationShouldThrowIfWriteFails() throws DataStorageException {
        doThrow(DataStorageException.class).when(mockDataStorage).append(any(), any());
        assertThrows(DataStorageException.class, () -> underTest.handleViolation(spark, testInputDataframe(), "source", "table", STRUCTURED_LOAD));
    }

    private Dataset<Row> testInputDataframe() {
        List<String> input = new ArrayList<>();
        input.add("data1,metadata1,other1");
        input.add("data2,metadata2,other2");
        return spark.createDataset(input, Encoders.STRING())
                .toDF()
                .selectExpr(
                        "split(value, ',')[0] as data",
                        "split(value, ',')[1] as metadata",
                        "split(value, ',')[2] as someothercolumn"
                );
    }
}