package uk.gov.justice.digital.zone.structured;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.writer.Writer;
import uk.gov.justice.digital.zone.Zone;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.SOURCE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TABLE;

public abstract class StructuredZoneS3 implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZoneS3.class);

    private final String structuredPath;
    private final String violationsPath;
    private final Writer writer;

    @Inject
    protected StructuredZoneS3(JobArguments jobArguments, Writer writer) {
        this.writer = writer;
        this.structuredPath = jobArguments.getStructuredS3Path();
        this.violationsPath = jobArguments.getViolationsS3Path();
    }

    public Dataset<Row> process(SparkSession spark, Dataset<Row> filteredRecords, SourceReference sourceReference) throws DataStorageException {
        return handleSchemaFound(spark, filteredRecords, sourceReference);
    }

    private Dataset<Row> handleSchemaFound(
            SparkSession spark,
            Dataset<Row> dataFrame,
            SourceReference sourceReference
    ) throws DataStorageException {
        val source = sourceReference.getSource();
        val table = sourceReference.getTable();

        val tablePath = createValidatedPath(structuredPath, source, table);
        val validationFailedViolationPath = createValidatedPath(violationsPath, source, table);

        if (dataFrameIsValid(dataFrame, sourceReference)) {
            val dataFrameWithDataFieldsOnly = dataFrame.drop(SOURCE, TABLE);
            handleValidRecords(spark, dataFrameWithDataFieldsOnly, tablePath, sourceReference.getPrimaryKey());
            return dataFrameWithDataFieldsOnly;
        } else {
            handleInvalidRecords(spark, dataFrame, source, table, validationFailedViolationPath);
            return spark.emptyDataFrame();
        }
    }

    private void handleInvalidRecords(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table,
            String destinationPath
    ) throws DataStorageException {
        val errorPrefix = String.format("Record does not match schema %s/%s: ", source, table);

        // Write invalid records where schema validation failed
        val invalidRecords = dataFrame.withColumn(ERROR, concat(lit(errorPrefix), lit(col(ERROR))));

            logger.warn("Violation - Records failed schema validation for source {}, table {}", source, table);
            writer.writeInvalidRecords(spark, destinationPath, invalidRecords);
    }

    private void handleValidRecords(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey
    ) throws DataStorageException {
        writer.writeValidRecords(spark, destinationPath, primaryKey, dataFrame);
    }

    private boolean dataFrameIsValid(Dataset<Row> dataFrame, SourceReference sourceReference) {
        val schemaFields = sourceReference.getSchema().fields();

        val dataFields = Arrays
                .stream(dataFrame.schema().fields())
                .collect(Collectors.toMap(StructField::name, StructField::dataType));

        val requiredFields = Arrays.stream(schemaFields)
                .filter(field -> !field.nullable())
                .collect(Collectors.toList());

        val missingRequiredFields = requiredFields
                .stream()
                .filter(field -> dataFields.get(field.name()) == null)
                .collect(Collectors.toList());

        val invalidRequiredFields = requiredFields
                .stream()
                .filter(field -> dataFields.get(field.name()) != field.dataType())
                .collect(Collectors.toList());

        val nullableFields = Arrays.stream(schemaFields)
                .filter(StructField::nullable)
                .collect(Collectors.toList());

        val invalidNullableFields = nullableFields
                .stream()
                .filter(field -> dataFields.get(field.name()) != field.dataType())
                .collect(Collectors.toList());

        return (missingRequiredFields.isEmpty() || invalidRequiredFields.isEmpty() || invalidNullableFields.isEmpty());
    }

}
