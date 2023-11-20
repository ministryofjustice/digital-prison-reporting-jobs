package uk.gov.justice.digital.service;

import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
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
        val maybeValidRows = validateRows(dataFrame, sourceReference);
        val validRows = maybeValidRows.filter("valid = true").drop("valid");
        val invalidRows = maybeValidRows.filter("valid = false").drop("valid");
        if(!invalidRows.isEmpty()) {
            try {
                violationService.handleInvalidSchema(spark, invalidRows, sourceReference.getSource(), sourceReference.getTable());
            } catch (DataStorageException e) {
                logger.error("Failed to write invalid rows");
                throw new RuntimeException(e);
            }
        }
        return validRows;
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
}
