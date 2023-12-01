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
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;

import javax.inject.Singleton;
import java.util.Arrays;

import static org.apache.spark.sql.functions.lit;

@Singleton
public class ValidationService {

    private static final Logger logger = LoggerFactory.getLogger(ValidationService.class);
    private final ViolationService violationService;

    @Inject
    public ValidationService(ViolationService violationService) {
        this.violationService = violationService;
    }

    public Dataset<Row> handleValidation(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) {
        try {
            val maybeValidRows = validateRows(dataFrame, sourceReference);
            val validRows = maybeValidRows.filter("valid = true").drop("valid");
            val invalidRows = maybeValidRows.filter("valid = false").drop("valid");
            if(!invalidRows.isEmpty()) {
                violationService.handleInvalidSchema(spark, invalidRows, sourceReference.getSource(), sourceReference.getTable());
            }
            return validRows;
        } catch (DataStorageException e) {
            logger.error("Failed to write invalid rows");
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    Dataset<Row> validateRows(Dataset<Row> df, SourceReference sourceReference) {
        val schemaFields = sourceReference.getSchema().fields();

        Column cond = allRequiredColumnsAreNotNull(schemaFields);
        return df.withColumn(
                "valid",
                cond
        );
    }

    private static Column allRequiredColumnsAreNotNull(StructField[] schemaFields) {
        return Arrays.stream(schemaFields)
                .filter(field -> !field.nullable())
                .map(StructField::name)
                .map(functions::col)
                .map(Column::isNotNull)
                .reduce(Column::and)
                .orElse(lit(true));
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
                if(!sameType&& !allowedDifference) {
                    return false;
                }
                if(inferredDataType instanceof StructType) {
                    // If it is a struct then recurse to check the nested types
                    if(!schemasMatch((StructType) inferredDataType, (StructType) specifiedDataType)) {
                        return false;
                    }
                }
            } catch (IllegalArgumentException e) {
                // No corresponding field with that name
                return false;
            }
        }
        return true;
    }
}
