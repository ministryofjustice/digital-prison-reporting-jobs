package uk.gov.justice.digital.service;

import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;

import javax.inject.Singleton;
import java.util.Arrays;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR;
import static uk.gov.justice.digital.common.CommonDataFields.withMetadataFields;

@Singleton
public class ValidationService {

    private static final Logger logger = LoggerFactory.getLogger(ValidationService.class);
    private final ViolationService violationService;
    @Inject
    public ValidationService(ViolationService violationService) {
        this.violationService = violationService;
    }

    public Dataset<Row> handleValidation(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference, StructType inferredSchema, ViolationService.ZoneName zoneName) {
        val maybeValidRows = validateRows(dataFrame, sourceReference, inferredSchema);
        val validRows = maybeValidRows.filter(col(ERROR).isNull()).drop(ERROR);
        val invalidRows = maybeValidRows.filter(col(ERROR).isNotNull());
        if(!invalidRows.isEmpty()) {
            violationService.handleViolation(spark, invalidRows, sourceReference.getSource(), sourceReference.getTable(), zoneName);
        }
        return validRows;
    }

    @VisibleForTesting
    Dataset<Row> validateRows(Dataset<Row> df, SourceReference sourceReference, StructType inferredSchema) {
        StructType schema = withMetadataFields(sourceReference.getSchema());

        if(schemasMatch(inferredSchema, schema)) {
            return df.withColumn(
                    ERROR,
                    // The order of the 'when' clauses determines the validation error message used - first wins.
                    // Null means there is no validation error.
                    when(pkIsNull(sourceReference), lit("Record does not have a primary key"))
                            .when(requiredColumnIsNull(schema.fields()), lit("Required column is null"))
                            .otherwise(lit(null))
            );
        } else {
            String msg = format("Record does not match schema version %s", sourceReference.getVersionId());
            logger.warn(msg + " Inferred schema:\n{}\nActual schema:\n{}\nFor {}.{}",
                    inferredSchema, schema, sourceReference.getSource(), sourceReference.getTable()
            );
            return df.withColumn(ERROR, lit(msg));
        }
    }

    @VisibleForTesting
    static boolean schemasMatch(StructType inferredSchema, StructType specifiedSchema) {
        if(inferredSchema.fields().length != specifiedSchema.fields().length) {
            return false;
        }
        for (StructField inferredField : inferredSchema.fields()) {
            try {
                StructField specifiedField = specifiedSchema.apply(inferredField.name());
                DataType inferredDataType = inferredField.dataType();
                DataType specifiedDataType = specifiedField.dataType();
                boolean sameType = specifiedDataType.getClass().equals(inferredDataType.getClass());
                // We represent shorts as ints in avro so this difference is allowed
                boolean allowedDifference = inferredDataType instanceof ShortType && specifiedDataType instanceof IntegerType;
                if(!sameType && !allowedDifference) {
                    return false;
                }
                // If it is a struct then recurse to check the nested types
                if(inferredDataType instanceof StructType &&
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
}
