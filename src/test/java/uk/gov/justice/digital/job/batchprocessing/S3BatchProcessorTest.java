package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoadS3;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoadS3;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;

@ExtendWith(MockitoExtension.class)
class S3BatchProcessorTest extends BaseSparkTest {

    private static final String table = "table";
    private static final String source = "source";
    private static final List<Row> inputRows = Arrays.asList(
            createRow(1, "2023-11-13 10:00:00.000000", Insert, "1"),
            createRow(2, "2023-11-13 10:00:00.000000", Insert, "2"),
            createRow(3, "2023-11-13 10:00:00.000000", Insert, "3")
    );
    private static final List<Row> validatedRows = Arrays.asList(
            createRow(1, "2023-11-13 10:00:00.000000", Insert, "1"),
            createRow(2, "2023-11-13 10:00:00.000000", Insert, "2")
    );

    private static Dataset<Row> inputDf;
    private static Dataset<Row> validatedDf;

    @Mock
    private StructuredZoneLoadS3 structuredZoneLoad;
    @Mock
    private CuratedZoneLoadS3 curatedZoneLoad;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private ValidationService validationService;

    private S3BatchProcessor underTest;

    @BeforeAll
    public static void setupClass() {
        inputDf =  spark.createDataFrame(inputRows, TEST_DATA_SCHEMA);
        validatedDf =  spark.createDataFrame(validatedRows, TEST_DATA_SCHEMA);
    }

    @BeforeEach
    public void setUp() {
        underTest = new S3BatchProcessor(structuredZoneLoad, curatedZoneLoad, validationService);
    }

    @Test
    public void shouldSkipProcessingForEmptyDataframe() throws DataStorageException {
        underTest.processBatch(spark, sourceReference, spark.emptyDataFrame());

        verify(structuredZoneLoad, times(0)).process(any(), any(), any());
        verify(curatedZoneLoad, times(0)).process(any(), any(), any());
    }
    @Test
    public void shouldProcessStructured() throws DataStorageException {
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        when(validationService.handleValidation(any(), any(), eq(sourceReference), eq(TEST_DATA_SCHEMA), eq(STRUCTURED_LOAD))).thenReturn(validatedDf);
        when(structuredZoneLoad.process(any(), any(), any())).thenReturn(validatedDf);

        underTest.processBatch(spark, sourceReference, inputDf);

        verify(structuredZoneLoad, times(1)).process(any(), argumentCaptor.capture(), eq(sourceReference));
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(validatedRows.size(), result.size());
        assertTrue(result.containsAll(validatedRows));

    }

    @Test
    public void shouldProcessCurated() throws DataStorageException {
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        when(validationService.handleValidation(any(), any(), eq(sourceReference), eq(TEST_DATA_SCHEMA), eq(STRUCTURED_LOAD)))
                .thenReturn(validatedDf);
        when(structuredZoneLoad.process(any(), any(), any())).thenReturn(validatedDf);

        underTest.processBatch(spark, sourceReference, inputDf);

        verify(curatedZoneLoad, times(1)).process(any(), argumentCaptor.capture(), eq(sourceReference));
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(validatedRows.size(), result.size());
        assertTrue(result.containsAll(validatedRows));
    }

    @Test
    public void shouldDelegateValidationOnlyValidatingInserts() throws DataStorageException {
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        Dataset<Row> mixedOperations = validatedDf
                .unionAll(validatedDf.withColumn(OPERATION, lit(Update.getName())))
                .unionAll(validatedDf.withColumn(OPERATION, lit(Delete.getName())));

        when(validationService.handleValidation(any(), any(), eq(sourceReference), eq(TEST_DATA_SCHEMA), eq(STRUCTURED_LOAD)))
                .thenReturn(validatedDf);
        when(structuredZoneLoad.process(any(), any(), any())).thenReturn(validatedDf);


        underTest.processBatch(spark, sourceReference, mixedOperations);

        verify(validationService, times(1))
                .handleValidation(any(), argumentCaptor.capture(), eq(sourceReference), eq(TEST_DATA_SCHEMA), eq(STRUCTURED_LOAD));
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(validatedRows.size(), result.size());
        assertTrue(result.containsAll(validatedRows));
    }
}