package uk.gov.justice.digital.zone.structured;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.job.udf.JsonValidator;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.writer.Writer;
import uk.gov.justice.digital.zone.Zone;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

public abstract class StructuredZone implements Zone, Serializable {

    private static final long serialVersionUID = 7305299867913939915L;

    public static final String ERROR = "error";
    public static final String PARSED_DATA = "parsedData";
    public static final String VALID = "valid";

    private static final Logger logger = LoggerFactory.getLogger(StructuredZone.class);

    private static final Map<String, String> jsonOptions = Collections.singletonMap("ignoreNullFields", "false");

    private final String structuredPath;
    private final String violationsPath;
    private final SourceReferenceService sourceReferenceService;
    private final Writer writer;

    @Inject
    public StructuredZone(
            JobArguments jobArguments,
            SourceReferenceService sourceReferenceService,
            Writer writer
    ) {
        this.writer = writer;
        this.structuredPath = jobArguments.getStructuredS3Path();
        this.violationsPath = jobArguments.getViolationsS3Path();
        this.sourceReferenceService = sourceReferenceService;
    }

    public Dataset<Row> process(SparkSession spark, Dataset<Row> filteredRecords, Row table) throws DataStorageException {

        val sortedRecords = filteredRecords.orderBy(col(TIMESTAMP));

        String sourceName = table.getAs(SOURCE);
        String tableName = table.getAs(TABLE);

        val sourceReference = sourceReferenceService.getSourceReference(sourceName, tableName);

        return sourceReference.isPresent()
                ? handleSchemaFound(spark, sortedRecords, sourceReference.get())
                : handleNoSchemaFound(spark, sortedRecords, sourceName, tableName);
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

        val validatedDataFrame = validateJsonData(spark, dataFrame, sourceReference.getSchema(), source, table);

        handleInvalidRecords(spark, validatedDataFrame, source, table, validationFailedViolationPath);

        return handleValidRecords(spark, validatedDataFrame, tablePath, sourceReference.getPrimaryKey());
    }

    private Dataset<Row> handleNoSchemaFound(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table
    ) throws DataStorageException {
        logger.warn("Violation - No schema found for {}/{}", source, table);

        val missingSchemaRecords = dataFrame
                .select(col(DATA), col(METADATA))
                .withColumn(ERROR, lit(String.format("Schema does not exist for %s/%s", source, table)));

        writer.writeInvalidRecords(spark, createValidatedPath(violationsPath, source, table), missingSchemaRecords);

        return createEmptyDataFrame(dataFrame);
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
        val invalidRecords = dataFrame
                .select(col(DATA), col(METADATA), col(VALID), col(ERROR))
                .filter(col(VALID).equalTo(false))
                .withColumn(ERROR, concat(lit(errorPrefix), lit(col(ERROR))))
                .drop(col(VALID));

        if (!invalidRecords.isEmpty()) {
            logger.warn("Violation - Records failed schema validation for source {}, table {}", source, table);
            writer.writeInvalidRecords(spark, destinationPath, invalidRecords);
        }
    }

    private Dataset<Row> handleValidRecords(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey
    ) throws DataStorageException {
        val validRecords = dataFrame.filter(col(VALID).equalTo(true)).select(PARSED_DATA + ".*", OPERATION);

        if (!validRecords.isEmpty()) {
            writer.writeValidRecords(spark, destinationPath, primaryKey, validRecords);

            return validRecords;
        } else {
            logger.warn("No valid records found");
            return createEmptyDataFrame(validRecords);
        }
    }

    private Dataset<Row> validateJsonData(
            SparkSession spark,
            Dataset<Row> dataFrame,
            StructType schema,
            String source,
            String table
    ) {
        logger.debug("Validating data against schema: {}/{}", source, table);
        val validator = JsonValidator.createAndRegister(schema, spark, source, table);

        return dataFrame
                .select(col(DATA), col(METADATA), col(OPERATION))
                .withColumn(PARSED_DATA, from_json(col(DATA), schema, jsonOptions))
                .withColumn(ERROR, validator.apply(col(DATA), to_json(col(PARSED_DATA), jsonOptions)))
                .withColumn(VALID, col(ERROR).equalTo(lit("")));
    }

    private Dataset<Row> createEmptyDataFrame(Dataset<Row> dataFrame) {
        return dataFrame.sparkSession().createDataFrame(
                dataFrame.sparkSession().emptyDataFrame().javaRDD(),
                dataFrame.schema()
        );
    }

}
