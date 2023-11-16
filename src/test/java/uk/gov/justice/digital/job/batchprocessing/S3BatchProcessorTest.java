package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
import uk.gov.justice.digital.service.SourceReferenceService;
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
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;

@ExtendWith(MockitoExtension.class)
class S3BatchProcessorTest extends BaseSparkTest {

    private static final String table = "table";
    private static final String source = "source";
    private static final List<Row> inputRows = Arrays.asList(
            RowFactory.create("1", "2023-11-13 10:00:00.000000", "I", "1"),
            RowFactory.create("2", "2023-11-13 10:00:00.000000", "I", "2"),
            RowFactory.create("3", "2023-11-13 10:00:00.000000", "I", "3")
    );
    private static final List<Row> validatedRows = Arrays.asList(
            RowFactory.create("1", "2023-11-13 10:00:00.000000", "I", "1"),
            RowFactory.create("2", "2023-11-13 10:00:00.000000", "I", "2")
    );

    private static Dataset<Row> inputDf;
    private static Dataset<Row> validatedDf;

    @Mock
    private StructuredZoneLoadS3 structuredZoneLoad;
    @Mock
    private CuratedZoneLoadS3 curatedZoneLoad;
    @Mock
    private SourceReferenceService sourceReferenceService;
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
        underTest = new S3BatchProcessor(structuredZoneLoad, curatedZoneLoad, sourceReferenceService, validationService);
        when(sourceReferenceService.getSourceReferenceOrThrow(source, table)).thenReturn(sourceReference);
    }

    @Test
    public void shouldProcessStructured() throws DataStorageException {
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        when(validationService.handleValidation(any(), any(), eq(sourceReference))).thenReturn(validatedDf);
        when(structuredZoneLoad.process(any(), any(), any())).thenReturn(validatedDf);

        underTest.processBatch(spark, source, table, inputDf);

        verify(structuredZoneLoad, times(1)).process(any(), argumentCaptor.capture(), eq(sourceReference));
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(validatedRows.size(), result.size());
        assertTrue(result.containsAll(validatedRows));

    }

    @Test
    public void shouldProcessCurated() throws DataStorageException {
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        when(validationService.handleValidation(any(), any(), eq(sourceReference))).thenReturn(validatedDf);
        when(structuredZoneLoad.process(any(), any(), any())).thenReturn(validatedDf);

        underTest.processBatch(spark, source, table, inputDf);

        verify(curatedZoneLoad, times(1)).process(any(), argumentCaptor.capture(), eq(sourceReference));
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(validatedRows.size(), result.size());
        assertTrue(result.containsAll(validatedRows));
    }

    @Test
    public void shouldDelegateValidationOnlyValidatingInserts() throws DataStorageException {
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        Dataset<Row> mixedOperations = validatedDf
                .unionAll(validatedDf.withColumn("Op", lit("U")))
                .unionAll(validatedDf.withColumn("Op", lit("D")));

        when(validationService.handleValidation(any(), any(), eq(sourceReference))).thenReturn(validatedDf);
        when(structuredZoneLoad.process(any(), any(), any())).thenReturn(validatedDf);


        underTest.processBatch(spark, source, table, mixedOperations);

        verify(validationService, times(1)).handleValidation(any(), argumentCaptor.capture(), eq(sourceReference));
        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(validatedRows.size(), result.size());
        assertTrue(result.containsAll(validatedRows));
    }
}