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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR;

@Singleton
public class ValidationService {

    private static final Logger logger = LoggerFactory.getLogger(ValidationService.class);
    private final ViolationService violationService;

    @Inject
    public ValidationService(ViolationService violationService) {
        this.violationService = violationService;
    }

    public Dataset<Row> handleValidation(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference, ViolationService.ZoneName zoneName) {
        try {
            val maybeValidRows = validateRows(dataFrame, sourceReference);
            val validRows = maybeValidRows.filter(col(ERROR).isNull()).drop(ERROR);
            val invalidRows = maybeValidRows.filter(col(ERROR).isNotNull());
            if(!invalidRows.isEmpty()) {
                violationService.handleViolation(spark, invalidRows, sourceReference.getSource(), sourceReference.getTable(), zoneName);
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

        return df.withColumn(
                ERROR,
                when(pkIsNull(sourceReference), lit("Record does not have a primary key"))
                        .when(requiredColumnIsNull(schemaFields), lit("Required column is null"))
                        .otherwise(lit(null))
        );
    }

    private static Column pkIsNull(SourceReference sourceReference) {
        return sourceReference
                .getPrimaryKey()
                .getKeyColumnNames()
                .stream()
                .map(pk -> col(pk).isNull())
                .reduce(Column::or)
                .orElse(lit(false));
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
