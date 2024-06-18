package uk.gov.justice.digital.service;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.*;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;
import static uk.gov.justice.digital.test.MinimalTestData.*;


@ExtendWith(MockitoExtension.class)
class ValidationServiceTest extends BaseSparkTest {

    private static final String source = "source";
    private static final String table = "table";
    private static final String requiredColumnIsNullMsg = "Required column is null";
    private static final String noPkMsg = "Record does not have a primary key";
    private static final String VERSION_ID = UUID.randomUUID().toString();
    private static final String schemaMisMatchMsg = "Record does not match schema version " + VERSION_ID;
    private static final String VALIDATION_TYPE = "{\"validationType\": \"time\"}";

    private static Dataset<Row> inputDf;
    @Mock
    private ViolationService violationService;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private SourceReference.PrimaryKey primaryKey;
    @Captor
    private ArgumentCaptor<Dataset<Row>> argumentCaptor;
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
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));

        List<Row> result = underTest.validateRows(inputDf, sourceReference, TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS).collectAsList();

        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:49:28.000000", "D", "data", CHECKPOINT_COL_VALUE, null),
                RowFactory.create(2, "2023-11-13 10:49:28.000000", "D", null, CHECKPOINT_COL_VALUE, null),
                RowFactory.create(3, "2023-11-13 10:49:29.000000", null, "data", CHECKPOINT_COL_VALUE, requiredColumnIsNullMsg),
                RowFactory.create(4, null, "I", "data", CHECKPOINT_COL_VALUE, requiredColumnIsNullMsg),
                RowFactory.create(5, null, null, "data", CHECKPOINT_COL_VALUE, requiredColumnIsNullMsg),
                RowFactory.create(6, "2023-11-13 10:49:29.000000", null, null, CHECKPOINT_COL_VALUE, requiredColumnIsNullMsg),
                RowFactory.create(7, null, null, null, CHECKPOINT_COL_VALUE, requiredColumnIsNullMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", "data", CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, null, "U", "data", CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", null, "data", CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", null, CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", null, null, CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, null, "U", null, CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, null, null, "data", CHECKPOINT_COL_VALUE, noPkMsg)
        );

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    public void validateRowsShouldValidateCompositePrimaryKeys() {
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Arrays.asList(PRIMARY_KEY_COLUMN, TIMESTAMP));

        List<Row> input = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.000000", Update, "data"),
                createRow(2, null, Update, null),
                createRow(null, "2023-11-13 10:49:29.000000", Update, "data"),
                createRow(null, null, Update, "data")
        );
        Dataset<Row> thisInputDf = spark.createDataFrame(input, TEST_DATA_SCHEMA);

        List<Row> result = underTest.validateRows(thisInputDf, sourceReference, TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS).collectAsList();

        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:49:28.000000", "U", "data", CHECKPOINT_COL_VALUE, null),
                RowFactory.create(2, null, "U", null, CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", "data", CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, null, "U", "data", CHECKPOINT_COL_VALUE, noPkMsg)
        );

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    public void validateRowsShouldValidateMismatchingSchemas() {
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getVersionId()).thenReturn(VERSION_ID);

        StructType misMatchingSchema = TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS.add(
                new StructField("extra-column", DataTypes.IntegerType, false, Metadata.empty())
        );

        List<Row> input = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.000000", Update, "data"),
                createRow(2, null, Update, null)
        );
        Dataset<Row> thisInputDf = spark.createDataFrame(input, TEST_DATA_SCHEMA);

        List<Row> result = underTest.validateRows(thisInputDf, sourceReference, misMatchingSchema).collectAsList();

        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:49:28.000000", "U", "data", CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(2, null, "U", null, CHECKPOINT_COL_VALUE, schemaMisMatchMsg)
        );

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    public void validateRowsShouldReturnInvalidWhenNoPrimaryKeyIsPresentInSchema() {
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.emptyList());

        List<Row> input = Collections.singletonList(
                createRow(1, "2023-11-13 10:49:28.000000", Update, "data")
        );
        Dataset<Row> thisInputDf = spark.createDataFrame(input, TEST_DATA_SCHEMA);

        List<Row> result = underTest.validateRows(thisInputDf, sourceReference, TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS).collectAsList();

        List<Row> expected = Collections.singletonList(
                RowFactory.create(1, "2023-11-13 10:49:28.000000", "U", "data", CHECKPOINT_COL_VALUE, noPkMsg)
        );

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    @Test
    void validateRowsShouldValidateTimeFields() {
        val timestamp = "2023-11-13 10:49:28.000000";
        
        val providedSchema = new StructType()
                .add(new StructField(PRIMARY_KEY_COLUMN, DataTypes.IntegerType, false, Metadata.empty()))
                .add(new StructField("time", DataTypes.StringType, false, Metadata.fromJson(VALIDATION_TYPE)))
                .add(new StructField("nullable_time", DataTypes.StringType, true, Metadata.fromJson(VALIDATION_TYPE)));

        val inferredSchema = withCheckpointField(withMetadataFields(
                new StructType()
                        .add(new StructField(PRIMARY_KEY_COLUMN, DataTypes.IntegerType, false, Metadata.empty()))
                        .add(new StructField("time", DataTypes.StringType, false, Metadata.fromJson(VALIDATION_TYPE)))
                        .add(new StructField("nullable_time", DataTypes.StringType, true, Metadata.fromJson(VALIDATION_TYPE)))
        ));

        when(sourceReference.getSchema()).thenReturn(providedSchema);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));

        val input = Arrays.asList(
                RowFactory.create(1, "09:00:00", "12:30:00", Insert.getName(), timestamp, CHECKPOINT_COL_VALUE),
                RowFactory.create(2, "23:59:59", null, Update.getName(), timestamp, CHECKPOINT_COL_VALUE),
                RowFactory.create(3, "00:00:00", "08:30:30", Delete.getName(), timestamp, CHECKPOINT_COL_VALUE)
        );

        val thisInputDf = spark.createDataFrame(input, inferredSchema);

        val result = underTest.validateRows(thisInputDf, sourceReference, inferredSchema).collectAsList();

        val expected = Arrays.asList(
                RowFactory.create(1, "09:00:00", "12:30:00", Insert.getName(), timestamp, CHECKPOINT_COL_VALUE, null),
                RowFactory.create(2, "23:59:59", null, Update.getName(), timestamp, CHECKPOINT_COL_VALUE, null),
                RowFactory.create(3, "00:00:00", "08:30:30", Delete.getName(), timestamp, CHECKPOINT_COL_VALUE, null)
        );

        assertEquals(expected.size(), result.size());
        assertThat(result, containsInAnyOrder(expected.toArray()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // Single digit field(s)
            "0:0:0", 
            "0:00:00",
            "00:0:00",
            "00:00:0",
            // Out of range
            "24:00:00",
            "00:00:60",
            "00:60:00",
            // Field exceeding two digits
            "123:00:00",
            "09:123:00",
            "09:00:123",
            // Other cases
            "09:00",
            "AB:CD:EF",
            " "
    })
    void validateRowsShouldRecordErrorWhenGivenInvalidTimeFields(String invalidTime) {
        val timestamp = "2023-11-13 10:49:28.000000";
        val validTime = "09:30:00";

        val providedSchema = new StructType()
                .add(new StructField(PRIMARY_KEY_COLUMN, DataTypes.IntegerType, false, Metadata.empty()))
                .add(new StructField("underscored_col", DataTypes.StringType, false, Metadata.fromJson(VALIDATION_TYPE)))
                .add(new StructField("hyphenated-col", DataTypes.StringType, true, Metadata.fromJson(VALIDATION_TYPE)));

        val inferredSchema = withCheckpointField(withMetadataFields(
                new StructType()
                        .add(new StructField(PRIMARY_KEY_COLUMN, DataTypes.IntegerType, false, Metadata.empty()))
                        .add(new StructField("underscored_col", DataTypes.StringType, false, Metadata.fromJson(VALIDATION_TYPE)))
                        .add(new StructField("hyphenated-col", DataTypes.StringType, true, Metadata.fromJson(VALIDATION_TYPE)))
        ));

        when(sourceReference.getSchema()).thenReturn(providedSchema);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));

        List<Row> input = Arrays.asList(
                RowFactory.create(1, invalidTime, invalidTime, Insert.getName(), timestamp, CHECKPOINT_COL_VALUE),
                RowFactory.create(2, validTime, null, Update.getName(), timestamp, CHECKPOINT_COL_VALUE)
        );
        
        Dataset<Row> thisInputDf = spark.createDataFrame(input, inferredSchema);

        List<Row> result = underTest.validateRows(thisInputDf, sourceReference, inferredSchema).collectAsList();

        String validationErrors = "hyphenated-col must have format HH:mm:ss; underscored_col must have format HH:mm:ss";
        List<Row> expected = Arrays.asList(
                RowFactory.create(1, invalidTime, invalidTime, Insert.getName(), timestamp, CHECKPOINT_COL_VALUE, validationErrors),
                RowFactory.create(2, validTime, null, Update.getName(), timestamp, CHECKPOINT_COL_VALUE, null)
        );

        assertEquals(expected.size(), result.size());
        assertThat(result, containsInAnyOrder(expected.toArray()));
    }

    @Test
    public void handleValidationShouldReturnValidRows() {
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(sourceReference.getSource()).thenReturn(source);
        when(sourceReference.getTable()).thenReturn(table);

        List<Row> result =
                underTest.handleValidation(spark, inputDf, sourceReference, TEST_DATA_SCHEMA, STRUCTURED_LOAD).collectAsList();

        List<Row> expectedValid = Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.000000", Delete, "data"),
                createRow(2, "2023-11-13 10:49:28.000000", Delete, null)
        );

        assertEquals(expectedValid.size(), result.size());
        assertTrue(result.containsAll(expectedValid));
    }

    @Test
    public void handleValidationShouldWriteViolationsWithInvalidRows() {
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(sourceReference.getSource()).thenReturn(source);
        when(sourceReference.getTable()).thenReturn(table);

        underTest.handleValidation(spark, inputDf, sourceReference, TEST_DATA_SCHEMA, STRUCTURED_LOAD).collectAsList();

        List<Row> expectedInvalid = Arrays.asList(
                RowFactory.create(3, "2023-11-13 10:49:29.000000", null, "data", CHECKPOINT_COL_VALUE, requiredColumnIsNullMsg),
                RowFactory.create(4, null, "I", "data", CHECKPOINT_COL_VALUE, requiredColumnIsNullMsg),
                RowFactory.create(5, null, null, "data", CHECKPOINT_COL_VALUE, requiredColumnIsNullMsg),
                RowFactory.create(6, "2023-11-13 10:49:29.000000", null, null, CHECKPOINT_COL_VALUE, requiredColumnIsNullMsg),
                RowFactory.create(7, null, null, null, CHECKPOINT_COL_VALUE, requiredColumnIsNullMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", "data", CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, null, "U", "data", CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", null, "data", CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", null, CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", null, null, CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, null, "U", null, CHECKPOINT_COL_VALUE, noPkMsg),
                RowFactory.create(null, null, null, "data", CHECKPOINT_COL_VALUE, noPkMsg)
        );

        verify(violationService, times(1)).handleViolation(any(), argumentCaptor.capture(), eq(source), eq(table), eq(STRUCTURED_LOAD));

        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(expectedInvalid.size(), result.size());
        assertTrue(result.containsAll(expectedInvalid));
    }

    @Test
    public void handleValidationShouldWriteWholeBatchAsViolationsForSchemaMisMatch() {
        StructType misMatchingSchema = TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS.add(
                new StructField("extra-column", DataTypes.IntegerType, false, Metadata.empty())
        );
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getVersionId()).thenReturn(VERSION_ID);
        when(sourceReference.getSource()).thenReturn(source);
        when(sourceReference.getTable()).thenReturn(table);

        underTest.handleValidation(spark, inputDf, sourceReference, misMatchingSchema, STRUCTURED_LOAD).collectAsList();

        List<Row> expectedInvalid = Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:49:28.000000", "D", "data", CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(2, "2023-11-13 10:49:28.000000", "D", null, CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(3, "2023-11-13 10:49:29.000000", null, "data", CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(4, null, "I", "data", CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(5, null, null, "data", CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(6, "2023-11-13 10:49:29.000000", null, null, CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(7, null, null, null, CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", "data", CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(null, null, "U", "data", CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", null, "data", CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", "U", null, CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(null, "2023-11-13 10:49:29.000000", null, null, CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(null, null, "U", null, CHECKPOINT_COL_VALUE, schemaMisMatchMsg),
                RowFactory.create(null, null, null, "data", CHECKPOINT_COL_VALUE, schemaMisMatchMsg)
        );

        verify(violationService, times(1)).handleViolation(any(), argumentCaptor.capture(), eq(source), eq(table), eq(STRUCTURED_LOAD));

        List<Row> result = argumentCaptor.getValue().collectAsList();
        assertEquals(expectedInvalid.size(), result.size());
        assertTrue(result.containsAll(expectedInvalid));
    }

    @Test
    public void handleValidationShouldThrowRTEWhenViolationServiceThrows() {
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(primaryKey.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(sourceReference.getSource()).thenReturn(source);
        when(sourceReference.getTable()).thenReturn(table);

        doThrow(new DataStorageException(""))
                .when(violationService)
                .handleViolation(any(), any(), any(), any(), any());
        assertThrows(RuntimeException.class, () ->
                underTest.handleValidation(spark, inputDf, sourceReference, TEST_DATA_SCHEMA, STRUCTURED_LOAD).collectAsList());
    }

}