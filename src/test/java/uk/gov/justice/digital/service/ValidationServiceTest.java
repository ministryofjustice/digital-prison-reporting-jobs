package uk.gov.justice.digital.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
import java.util.Collections;
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
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
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
    @Mock
    private SourceReference.PrimaryKey primaryKey;
    private ValidationService underTest;

    @BeforeEach
    public void setUp() {
        underTest = new ValidationService(violationService);
        List<Row> input = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.000000", Delete, "data"),
                createRow(2, "2023-11-13 10:49:28.000000", Delete, null),
                createRow(3, "2023-11-13 10:49:29.000000", null, "data"),
                createRow(4, null, Insert, "data"),
                createRow(5, null, null, "data"),
                createRow(6, "2023-11-13 10:49:29.000000", null, null),
                createRow(7, null, null, null),
                createRow(null, "2023-11-13 10:49:29.000000", Update, "data"),
                createRow(null, null, Update, "data"),
                createRow(null, "2023-11-13 10:49:29.000000", null, "data"),
                createRow(null, "2023-11-13 10:49:29.000000", Update, null),
                createRow(null, "2023-11-13 10:49:29.000000", null, null),
                createRow(null, null, Update, null),
                createRow(null, null, null, "data")
        );
        inputDf = spark.createDataFrame(input, TEST_DATA_SCHEMA);
    }

    @Test
    public void validateRowsShouldValidateBasedOnNullPrimaryKeysAndNonNullColumns() {
        when(sourceReference.getSchema()).thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));

        List<Row> result = underTest.validateRows(inputDf, sourceReference).collectAsList();

        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:49:28.000000", "D", "data", null),
                RowFactory.create(2, "2023-11-13 10:49:28.000000", "D", null, null),
                RowFactory.create(3, "2023-11-13 10:49:29.000000", null, "data", "Required column is null"),
                RowFactory.create(4, null, "I", "data", "Required column is null"),
                RowFactory.create(5, null, null, "data", "Required column is null"),
                RowFactory.create(6, "2023-11-13 10:49:29.000000", null, null, "Required column is null"),
                RowFactory.create(7, null, null, null, "Required column is null"),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", "data", "Record does not have a primary key"),
                RowFactory.create(null, null, "U", "data", "Record does not have a primary key"),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", null, "data", "Record does not have a primary key"),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", null, "Record does not have a primary key"),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", null, null, "Record does not have a primary key"),
                RowFactory.create(null, null, "U", null, "Record does not have a primary key"),
                RowFactory.create(null, null, null, "data", "Record does not have a primary key")
        );

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    public void validateRowsShouldValidateCompositePrimaryKeys() {
        when(sourceReference.getSchema()).thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Arrays.asList(PRIMARY_KEY_COLUMN, TIMESTAMP));

        List<Row> input = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.000000", Update, "data"),
                createRow(2, null, Update, null),
                createRow(null, "2023-11-13 10:49:29.000000", Update, "data"),
                createRow(null, null, Update, "data")
        );
        Dataset<Row> thisInputDf = spark.createDataFrame(input, TEST_DATA_SCHEMA);

        List<Row> result = underTest.validateRows(thisInputDf, sourceReference).collectAsList();

        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:49:28.000000", "U", "data", null),
                RowFactory.create(2, null, "U", null, "Record does not have a primary key"),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", "data", "Record does not have a primary key"),
                RowFactory.create(null, null, "U", "data", "Record does not have a primary key")
        );

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    public void handleValidationShouldReturnValidRows() {
        when(sourceReference.getSchema()).thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(sourceReference.getSource()).thenReturn(source);
        when(sourceReference.getTable()).thenReturn(table);

        List<Row> result = underTest.handleValidation(spark, inputDf, sourceReference, STRUCTURED_LOAD).collectAsList();

        List<Row> expectedValid = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.000000", Delete, "data"),
                createRow(2, "2023-11-13 10:49:28.000000", Delete, null)
        );

        assertEquals(expectedValid.size(), result.size());
        assertTrue(result.containsAll(expectedValid));
    }

    @Test
    public void handleValidationShouldWriteViolationsWithInvalidRows() throws DataStorageException {
        when(sourceReference.getSchema()).thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(sourceReference.getSource()).thenReturn(source);
        when(sourceReference.getTable()).thenReturn(table);
        ArgumentCaptor<Dataset<Row>> argumentCaptor = ArgumentCaptor.forClass(Dataset.class);

        underTest.handleValidation(spark, inputDf, sourceReference, STRUCTURED_LOAD).collectAsList();

        List<Row> expectedInvalid = Arrays.asList(
                RowFactory.create(3, "2023-11-13 10:49:29.000000", null, "data", "Required column is null"),
                RowFactory.create(4, null, "I", "data", "Required column is null"),
                RowFactory.create(5, null, null, "data", "Required column is null"),
                RowFactory.create(6, "2023-11-13 10:49:29.000000", null, null, "Required column is null"),
                RowFactory.create(7, null, null, null, "Required column is null"),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", "data", "Record does not have a primary key"),
                RowFactory.create(null, null, "U", "data", "Record does not have a primary key"),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", null, "data", "Record does not have a primary key"),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", null, "Record does not have a primary key"),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", null, null, "Record does not have a primary key"),
                RowFactory.create(null, null, "U", null, "Record does not have a primary key"),
                RowFactory.create(null, null, null, "data", "Record does not have a primary key")
        );

        verify(violationService, times(1)).handleViolation(any(), argumentCaptor.capture(), eq(source), eq(table), eq(STRUCTURED_LOAD));

        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(expectedInvalid.size(), result.size());
        assertTrue(result.containsAll(expectedInvalid));
    }

    @Test
    public void handleValidationShouldThrowRTEWhenViolationServiceThrows() throws DataStorageException {
        when(sourceReference.getSchema()).thenReturn(TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(sourceReference.getSource()).thenReturn(source);
        when(sourceReference.getTable()).thenReturn(table);

        doThrow(new DataStorageException(""))
                .when(violationService)
                .handleViolation(any(), any(), any(), any(), any());
        assertThrows(RuntimeException.class, () -> underTest.handleValidation(spark, inputDf, sourceReference, STRUCTURED_LOAD).collectAsList());
    }

}