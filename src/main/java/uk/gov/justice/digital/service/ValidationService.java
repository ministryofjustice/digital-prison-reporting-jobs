package uk.gov.justice.digital.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;

import javax.inject.Singleton;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.common.CommonDataFields.*;

@Singleton
public class ValidationService {

    private static final Logger logger = LoggerFactory.getLogger(ValidationService.class);
    private final ViolationService violationService;

    private final ImmutableMap<String, String> validationFormats = ImmutableMap.<String, String>builder()
            .put("time", "^(?:[01]\\d|2[0-3]):(?:[0-5]\\d|5[0-9]):(?:[0-5]\\d|5[0-9])$") // HH:mm:ss
            .build();

    @Inject
    public ValidationService(ViolationService violationService) {
        this.violationService = violationService;
    }

    public Dataset<Row> handleValidation(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference, StructType inferredSchema, ViolationService.ZoneName zoneName) {
        val maybeValidRows = validateRows(dataFrame, sourceReference, inferredSchema);
        val validRows = maybeValidRows.filter(col(ERROR).isNull()).drop(ERROR);
        val invalidRows = maybeValidRows.filter(col(ERROR).isNotNull());
        if (!invalidRows.isEmpty()) {
            violationService.handleViolation(spark, invalidRows, sourceReference.getSource(), sourceReference.getTable(), zoneName);
        }
        return validRows;
    }

    @VisibleForTesting
    Dataset<Row> validateRows(Dataset<Row> df, SourceReference sourceReference, StructType inferredSchema) {
        StructType schema = withMetadataFields(sourceReference.getSchema());
        val validatedDf = validateStringFields(df, sourceReference);
        if (schemasMatch(inferredSchema, schema)) {
            return validatedDf.withColumn(
                    ERROR,
                    // The order of the 'when' clauses determines the validation error message used - first wins.
                    // Null means there is no validation error.
                    when(pkIsNull(sourceReference), concatenateErrors("Record does not have a primary key"))
                            .when(requiredColumnIsNull(schema.fields()), concatenateErrors("Required column is null"))
                            .otherwise(col(ERROR))
            );
        } else {
            String msg = format("Record does not match schema version %s", sourceReference.getVersionId());
            logger.warn(msg + " Inferred schema:\n{}\nActual schema:\n{}\nFor {}.{}",
                    inferredSchema, schema, sourceReference.getSource(), sourceReference.getTable()
            );
            return validatedDf.withColumn(ERROR, concatenateErrors(msg));
        }
    }

    @VisibleForTesting
    static boolean schemasMatch(StructType inferredSchema, StructType specifiedSchema) {
        if (inferredSchema.fields().length != specifiedSchema.fields().length) {
            return false;
        }
        for (StructField inferredField : inferredSchema.fields()) {
            try {
                StructField specifiedField = specifiedSchema.apply(inferredField.name());
                DataType inferredDataType = inferredField.dataType();
                DataType specifiedDataType = specifiedField.dataType();
                boolean sameType = specifiedDataType.getClass().equals(inferredDataType.getClass());

                if (!sameType && !isAllowedDifference(inferredDataType, specifiedDataType)) {
                    return false;
                }
                // If it is a struct then recurse to check the nested types
                if (inferredDataType instanceof StructType &&
                        !schemasMatch((StructType) inferredDataType, (StructType) specifiedDataType)) {
                    // The struct schemas don't recursively match so the overall schema doesn't match
                    return false;
                }
            } catch (IllegalArgumentException e) {
                // No corresponding field with that name
                return false;
            }
        }
        return true;
    }

    private Dataset<Row> validateStringFields(Dataset<Row> df, SourceReference sourceReference) {
        val fieldsWithValidationMetadata = Arrays.stream(sourceReference.getSchema().fields())
                .filter(field -> field.dataType() instanceof StringType && field.metadata().contains(VALIDATION_TYPE_KEY))
                .distinct()
                .collect(Collectors.toMap(StructField::name, field -> field.metadata().getString(VALIDATION_TYPE_KEY)));

        Column[] fieldValidationResults = fieldsWithValidationMetadata
                .entrySet()
                .stream()
                .map(entry -> validateField(entry.getKey(), entry.getValue()))
                .toArray(Column[]::new);
        
        Column concatenatedErrors = concat_ws("; ", fieldValidationResults);
        return df.withColumn(ERROR, when(concatenatedErrors.eqNullSafe(lit("")), lit(null)).otherwise(concatenatedErrors));
    }

    // Handles special cases, e.g. due to minor differences in data types used in Parquet/Spark and Avro schemas
    private static boolean isAllowedDifference(DataType inferredDataType, DataType specifiedDataType) {
        // We represent 8 and 16 bit ints as 32 bit ints in avro so this difference is allowed
        return (inferredDataType instanceof ShortType && specifiedDataType instanceof IntegerType) ||
                (inferredDataType instanceof ByteType && specifiedDataType instanceof IntegerType);
    }

    private static Column pkIsNull(SourceReference sourceReference) {
        return sourceReference
                .getPrimaryKey()
                .getKeyColumnNames()
                .stream()
                .map(pk -> col(pk).isNull())
                .reduce(Column::or)
                .orElse(lit(true));
    }

    private static Column requiredColumnIsNull(StructField[] schemaFields) {
        return Arrays.stream(schemaFields)
                .filter(field -> !field.nullable())
                .map(StructField::name)
                .map(functions::col)
                .map(Column::isNull)
                .reduce(Column::or)
                .orElse(lit(false));
    }

    private Column validateField(String fieldName, String validationType) {
        return when(
                col(fieldName).isNotNull().and(not(col(fieldName).rlike(getValidationFormat(validationType)))),
                lit(fieldName + " must have format HH:mm:ss")
        );
    }

    private static Column concatenateErrors(String errorMsg) {
        return when(col(ERROR).isNull(), lit(errorMsg)).otherwise(concat(col(ERROR), lit("; "), lit(errorMsg)));
    }
    
    private String getValidationFormat(String key) {
        return Optional.ofNullable(validationFormats.get(key))
                .orElseThrow(() -> 
                        new RuntimeException(
                                String.format("Invalid field validation type %s. Allowed values are: %s", key, validationFormats.keySet())
                        )
                );
    }
}
