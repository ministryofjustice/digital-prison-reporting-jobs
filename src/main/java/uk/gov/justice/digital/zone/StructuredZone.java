package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.job.udf.JsonValidator;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.model.SourceReference;

import javax.inject.Inject;
import javax.inject.Singleton;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.job.model.Columns.*;

@Singleton
public class StructuredZone extends Zone {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZone.class);

    private final String structuredPath;
    private final String violationsPath;

    @Inject
    public StructuredZone(JobParameters jobParameters) {
        this.structuredPath = jobParameters.getStructuredS3Path()
            .orElseThrow(() -> new IllegalStateException(
                "structured s3 path not set - unable to create StructuredZone instance"
            ));
        this.violationsPath = jobParameters.getViolationsS3Path()
            .orElseThrow(() -> new IllegalStateException(
                "violations s3 path not set - unable to create StructuredZone instance"
            ));
    }

    @Override
    public Dataset<Row> process(Dataset<Row> dataFrame, Row table) {

        long noOfRows = dataFrame.count();
        logger.info("Processing batch with " + noOfRows + " records");

        val startTime = System.currentTimeMillis();

        String sourceName = table.getAs(SOURCE);
        String tableName = table.getAs(TABLE);

        val sourceReference = SourceReferenceService.getSourceReference(sourceName, tableName);

        // Filter records on table name in metadata
        //val dataFrameForTable = dataFrame.filter(col("metadata").ilike("%" + sourceName + "." + tableName + "%"));
        // TODO extract this dataFrame.count() into a variable
        logger.info("Processing {} records for {}/{}", noOfRows, sourceName, tableName);

        Dataset<Row> validStructuredDataFrame;

        if (sourceReference.isPresent()){
            validStructuredDataFrame = handleSchemaFound(dataFrame, sourceReference.get());
        } else {
            validStructuredDataFrame = handleNoSchemaFound(dataFrame, sourceName, tableName);
        }


        logger.info("Processed data frame with {} rows in {}ms",
                noOfRows,
            System.currentTimeMillis() - startTime
        );
        return validStructuredDataFrame;
    }

    private Dataset<Row> handleSchemaFound(Dataset<Row> dataFrame, SourceReference sourceReference) {
        val tablePath = getTablePath(structuredPath, sourceReference);
        val validationFailedViolationPath = getTablePath(
            violationsPath,
            sourceReference
        );
        val validatedDataFrame = validateJsonData(dataFrame, sourceReference.getSchema(), sourceReference.getSource(), sourceReference.getTable());
        val validDataFrame = handleValidRecords(validatedDataFrame, tablePath);
        handleInValidRecords(validatedDataFrame, sourceReference.getSource(), sourceReference.getTable(), validationFailedViolationPath);
        return validDataFrame;
    }

    private Dataset<Row> validateJsonData(Dataset<Row> dataFrame, StructType schema, String source, String table) {
        logger.info("Validating data against schema: {}/{}", source, table);

        val jsonValidator = JsonValidator.createAndRegister(schema, dataFrame.sparkSession(), source, table);

        return dataFrame
            .select(col(DATA), col(METADATA))
            .withColumn(PARSED_DATA, from_json(col(DATA), schema))
            .withColumn(VALID, jsonValidator.apply(col(DATA), to_json(col(PARSED_DATA))));
    }

    private Dataset<Row> handleValidRecords(Dataset<Row> dataFrame, String destinationPath) {
        val validRecords = dataFrame
            .select(col(PARSED_DATA), col(VALID))
            .filter(col(VALID).equalTo(true))
            .select(PARSED_DATA + ".*");

        if (validRecords.count() > 0) {
            logger.info("Writing {} valid records", validRecords.count());

            appendToDeltaLakeTable(validRecords, destinationPath);
            return validRecords;
        } else {
            // TODO return empty dataframe
            return null;
        }
    }

    private void handleInValidRecords(Dataset<Row> dataFrame, String source, String table, String destinationPath) {
        val errorString = String.format("Record does not match schema %s/%s", source, table);

        // Write invalid records where schema validation failed
        val invalidRecords = dataFrame
            .select(col(DATA), col(METADATA), col(VALID))
            .filter(col(VALID).equalTo(false))
            .withColumn(ERROR, lit(errorString))
            .drop(col(VALID));

        if (invalidRecords.count() > 0) {
            logger.error("Structured Zone Violation - {} records failed schema validation", invalidRecords.count());
            appendToDeltaLakeTable(invalidRecords, destinationPath);
        }
    }

    private Dataset<Row> handleNoSchemaFound(Dataset<Row> dataFrame, String source, String table) {
        logger.error("Structured Zone Violation - No schema found for {}/{} - writing {} records",
            source,
            table,
            dataFrame.count()
        );

        val missingSchemaRecords = dataFrame
            .select(col(DATA), col(METADATA))
            .withColumn(ERROR, lit(String.format("Schema does not exist for %s/%s", source, table)));

        appendToDeltaLakeTable(missingSchemaRecords, getTablePath(violationsPath, source, table));
        // TODO return empty dataframe
        return null;
    }

}
