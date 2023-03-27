package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.job.udf.JsonValidator;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.model.SourceReference;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.job.model.Columns.*;

@Singleton
public class StructuredZone implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZone.class);

    // TODO - this duplicates the constants in RawZone
    // TODO - ensure we only process load events for now
    private static final String PATH = "path";
    // TODO - enum?
    public static final String LOAD = "load";
    private final String structuredS3Path;

    @Inject
    public StructuredZone(JobParameters jobParameters) {
        // TODO - this needs to be the path to the root location where structured data and violations will be written
        this.structuredS3Path = jobParameters.getStructuredS3Path()
            .orElseThrow(() -> new IllegalStateException(
                "structured s3 path not set - unable to create StructuredZone instance"
            ));
    }

    @Override
    public void process(Dataset<Row> dataFrame) {

        logger.info("Processing batch with " + dataFrame.count() + " records");

        val startTime = System.currentTimeMillis();

        uniqueTablesForLoad(dataFrame).forEach((table) -> {
            String sourceName = table.getAs(SOURCE);
            String tableName = table.getAs(TABLE);

            val sourceReference = SourceReferenceService.getSourceReference(sourceName, tableName);

            // Filter records on table name in metadata
            // TODO - filter on load too
            val dataFrameForTable = dataFrame.filter(col("metadata").ilike("%" + sourceName + "." + tableName + "%"));

            logger.info("Processing {} records for {}/{}", dataFrameForTable.count(), sourceName, tableName);

            if (sourceReference.isPresent()) handleSchemaFound(dataFrameForTable, sourceReference.get());
            else handleNoSchemaFound(dataFrameForTable, sourceName, tableName);
        });

        logger.info("Processed data frame with {} rows in {}ms",
            dataFrame.count(),
            System.currentTimeMillis() - startTime
        );
    }

    private void handleSchemaFound(Dataset<Row> dataFrame, SourceReference sourceReference) {
        // TODO - fix this (varargs?)
        val tablePath = getTablePath(structuredS3Path, sourceReference.getSource(), sourceReference.getTable(), "");
        val validationFailedViolationPath = getTablePath(
            structuredS3Path,
            "violations",
            sourceReference.getSource(),
            sourceReference.getTable()
        );
        val validatedDataFrame = validateJsonData(dataFrame, sourceReference.getSchema(), sourceReference.getSource(), sourceReference.getTable());
        handleValidRecords(validatedDataFrame, tablePath);
        handleInValidRecords(validatedDataFrame, sourceReference.getSource(), sourceReference.getTable(), validationFailedViolationPath);
    }

    private Dataset<Row> validateJsonData(Dataset<Row> dataFrame, StructType schema, String source, String table) {
        logger.info("Validating data against schema: {}/{}", source, table);

        val jsonValidator = JsonValidator.createAndRegister(schema, dataFrame.sparkSession(), source, table);

        return dataFrame
            .select(col(DATA), col(METADATA))
            .withColumn(PARSED_DATA, from_json(col(DATA), schema))
            .withColumn(VALID, jsonValidator.apply(col(DATA), to_json(col(PARSED_DATA))));
    }

    private void handleValidRecords(Dataset<Row> dataFrame, String destinationPath) {
        val validRecords = dataFrame
            .select(col(PARSED_DATA), col(VALID))
            .filter(col(VALID).equalTo(true))
            .select(PARSED_DATA + ".*");

        if (validRecords.count() > 0) {
            logger.info("Writing {} valid records", validRecords.count());

            validRecords
                .write()
                .mode(SaveMode.Append)
                .option(PATH, destinationPath)
                .format("delta")
                .save();
        }
    }

    private void handleInValidRecords(Dataset<Row> dataFrame, String source, String table, String destinationPath) {
        val errorString = String.format(
            "Record does not match schema %s/%s",
            source,
            table
        );

        // Write invalid records where schema validation failed
        val invalidRecords = dataFrame
            .select(col(DATA), col(METADATA), col(VALID))
            .filter(col(VALID).equalTo(false))
            .withColumn(ERROR, lit(errorString))
            .drop(col(VALID));

        if (invalidRecords.count() > 0) {

            logger.error("Structured Zone Violation - {} records failed schema validation",
                invalidRecords.count()
            );

            invalidRecords
                .write()
                .mode(SaveMode.Append)
                .option(PATH, destinationPath)
                .format("delta")
                .save();
        }
    }

    private void handleNoSchemaFound(Dataset<Row> dataFrame, String source, String table) {
        logger.error("Structured Zone Violation - No schema found for {}/{} - writing {} records",
            source,
            table,
            dataFrame.count()
        );

        dataFrame
            .select(col(DATA), col(METADATA))
            .withColumn(ERROR, lit(String.format("Schema does not exist for %s/%s", source, table)))
            .write()
            .mode(SaveMode.Append)
            .option(PATH, getTablePath(structuredS3Path, "violations", source, table))
            .format("delta")
            .save();
    }

    // TODO - duplicated from RawZone
    // TODO - better names
    private List<Row> uniqueTablesForLoad(Dataset<Row> dataFrame) {
        return dataFrame
            .filter(col(OPERATION).isin(LOAD))
            .select(TABLE, SOURCE, OPERATION)
            .distinct()
            .collectAsList();
    }

}
