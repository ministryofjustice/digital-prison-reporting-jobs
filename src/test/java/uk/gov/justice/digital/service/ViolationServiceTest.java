package uk.gov.justice.digital.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR_RAW;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.RAW;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;
import static uk.gov.justice.digital.test.MinimalTestData.inserts;

@ExtendWith(MockitoExtension.class)
class ViolationServiceTest extends SparkTestBase {

    private static final String violationsPath = "s3://some-path";

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private DataStorageService mockDataStorage;
    @Mock
    private S3DataProvider dataProvider;
    @Mock
    private TableDiscoveryService tableDiscoveryService;
    @Mock
    private DataStorageRetriesExhaustedException mockCause;
    @Captor
    private ArgumentCaptor<Dataset<Row>> argumentCaptor;

    private ViolationService underTest;

    @BeforeEach
    void setUp() {
        when(mockJobArguments.getViolationsS3Path()).thenReturn(violationsPath);
        underTest = new ViolationService(mockJobArguments, mockDataStorage, dataProvider, tableDiscoveryService);
    }

    @Test
    void handleRetriesExhaustedShouldWriteViolations() {
        Dataset<Row> inputDf = inserts(spark);
        underTest.handleRetriesExhausted(spark, inputDf, "source", "table", mockCause, STRUCTURED_LOAD);
        verify(mockDataStorage).append(eq("s3://some-path/structured/source/table"), any());
        verify(mockDataStorage).updateDeltaManifestForTable(any(), any());
    }

    @Test
    void handleRetriesExhaustedShouldThrowIfWriteFails() {
        Dataset<Row> inputDf = inserts(spark);
        doThrow(DataStorageException.class).when(mockDataStorage).append(any(), any());
        assertThrows(RuntimeException.class, () -> underTest.handleRetriesExhausted(spark, inputDf, "source", "table", mockCause, RAW));
    }

    @Test
    void handleNoSchemaFoundShouldWriteViolations() {
        underTest.handleNoSchemaFound(spark, testInputDataframe(), "source", "table", STRUCTURED_LOAD);
        verify(mockDataStorage).append(eq("s3://some-path/structured/source/table"), any());
        verify(mockDataStorage).updateDeltaManifestForTable(any(), any());
    }

    @Test
    void handleNoSchemaFoundShouldThrowIfWriteFails() {
        doThrow(DataStorageException.class).when(mockDataStorage).append(any(), any());
        Dataset<Row> df = testInputDataframe();
        assertThrows(DataStorageException.class, () -> underTest.handleNoSchemaFound(spark, df, "source", "table", STRUCTURED_LOAD));
    }

    @Test
    void handleInvalidSchemaShouldThrowIfWriteFails() {
        doThrow(DataStorageException.class).when(mockDataStorage).append(any(), any());
        Dataset<Row> df = testInputDataframe();
        assertThrows(DataStorageException.class, () -> underTest.handleViolation(spark, df, "source", "table", STRUCTURED_LOAD));
    }

    @Test
    void handleViolationShouldWriteViolations() {
        underTest.handleViolation(spark, testInputDataframe(), "source", "table", STRUCTURED_LOAD);
        verify(mockDataStorage).append(eq("s3://some-path/structured/source/table"), any());
        verify(mockDataStorage).updateDeltaManifestForTable(any(), any());
    }

    @Test
    void handleViolationShouldWriteViolationsUsingCommonSchema() {
        underTest.handleViolation(spark, testInputDataframe(), "source", "table", STRUCTURED_LOAD);
        verify(mockDataStorage).append(any(), argumentCaptor.capture());

        Dataset<Row> result = argumentCaptor.getValue();
        List<String> expectedColumns = Arrays.asList(ERROR, ERROR_RAW);
        List<String> actualColumns = Arrays.asList(result.columns());

        assertEquals(expectedColumns.size(), actualColumns.size());
        assertTrue(expectedColumns.containsAll(actualColumns));

        assertEquals(DataTypes.StringType, result.schema().apply(ERROR).dataType());
        assertEquals(DataTypes.StringType, result.schema().apply(ERROR_RAW).dataType());
    }

    @Test
    void handleViolationShouldThrowIfWriteFails() {
        doThrow(DataStorageException.class).when(mockDataStorage).append(any(), any());
        Dataset<Row> df = testInputDataframe();
        assertThrows(DataStorageException.class, () -> underTest.handleViolation(spark, df, "source", "table", STRUCTURED_LOAD));
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
                )
                .withColumn(ERROR, lit("some error"));
    }
}
