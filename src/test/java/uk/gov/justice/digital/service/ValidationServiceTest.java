package uk.gov.justice.digital.service;

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

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;


@ExtendWith(MockitoExtension.class)
class ValidationServiceTest extends BaseSparkTest {

    private static final String source = "source";
    private static final String table = "table";
    private static Dataset<Row> inputDf;
    @Mock
    private ViolationService violationService;
    @Mock
    private SourceReference sourceReference;
    private ValidationService underTest;

    @BeforeAll
    public static void setUpTest() {
        List<Row> input = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.000000", Delete, null),
                createRow(2, "2023-11-13 10:49:29.000000", null, "2a"),
                createRow(3, null, Insert, "2a"),
                createRow(null, "2023-11-13 10:49:29.000000", Update, "2a")
        );
        inputDf = spark.createDataFrame(input, TEST_DATA_SCHEMA);
    }

    @BeforeEach
    public void setUp() {
        underTest = new ValidationService(violationService);

        when(sourceReference.getSchema()).thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
    }

    @Test
    public void validateRowsShouldValidateBasedOnNullability() {
        List<Row> result = underTest.validateRows(inputDf, sourceReference).collectAsList();

        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:49:28.000000", "D", null, true),
                RowFactory.create(2, "2023-11-13 10:49:29.000000", null, "2a", false),
                RowFactory.create(3, null, "I", "2a", false),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", "2a", false)
        );

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    public void handleValidationShouldReturnValidRows() {
        when(sourceReference.getSource()).thenReturn(source);
        when(sourceReference.getTable()).thenReturn(table);

        List<Row> result = underTest.handleValidation(spark, inputDf, sourceReference, STRUCTURED_LOAD).collectAsList();

        List<Row> expectedValid = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.000000", Delete, null)
        );

        assertEquals(expectedValid.size(), result.size());
        assertTrue(result.containsAll(expectedValid));
    }

    @Test
    public void handleValidationShouldWriteViolationsWithInvalidRows() throws DataStorageException {
        when(sourceReference.getSource()).thenReturn(source);
        when(sourceReference.getTable()).thenReturn(table);
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        underTest.handleValidation(spark, inputDf, sourceReference, STRUCTURED_LOAD).collectAsList();

        List<Row> expectedInvalid = Arrays.asList(
                createRow(2, "2023-11-13 10:49:29.000000", null, "2a"),
                createRow(3, null, Insert, "2a"),
                createRow(null, "2023-11-13 10:49:29.000000", Update, "2a")
        );

        verify(violationService, times(1)).handleInvalidSchema(any(), argumentCaptor.capture(), eq(source), eq(table), eq(STRUCTURED_LOAD));

        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(expectedInvalid.size(), result.size());
        assertTrue(result.containsAll(expectedInvalid));
    }

    @Test
    public void handleValidationShouldThrowRTEWhenViolationServiceThrows() throws DataStorageException {
        when(sourceReference.getSource()).thenReturn(source);
        when(sourceReference.getTable()).thenReturn(table);

        doThrow(new DataStorageException(""))
                .when(violationService)
                .handleInvalidSchema(any(), any(), any(), any(), any());
        assertThrows(RuntimeException.class, () -> underTest.handleValidation(spark, inputDf, sourceReference, STRUCTURED_LOAD).collectAsList());
    }

}