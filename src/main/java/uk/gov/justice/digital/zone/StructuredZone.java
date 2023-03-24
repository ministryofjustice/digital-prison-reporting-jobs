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

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

import static org.apache.spark.sql.functions.*;

@Singleton
public class StructuredZone implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZone.class);

    // TODO - this duplicates the constants in RawZone
    // TODO - ensure we only process load events for now
    private static final String LOAD = "load";
    private static final String SOURCE = "source";
    private static final String TABLE = "table";
    private static final String OPERATION = "operation";
    private static final String PATH = "path";

    private final String structuredS3Path;

    @Inject
    public StructuredZone(JobParameters jobParameters) {
        // TODO - this needs to be the path to the root location where structured data and violations will be written
        this.structuredS3Path = jobParameters.getStructuredS3Path()
            .orElseThrow(() -> new IllegalStateException(
                "structured s3 path not set - unable to create StructuredZone instance"
            ));
    }

    // TODO - filter on load events too
    @Override
    public void process(Dataset<Row> dataFrame) {

        logger.info("Processing batch with " + dataFrame.count() + " records");

        val startTime = System.currentTimeMillis();

        uniqueTablesForLoad(dataFrame).forEach((table) -> {
            // Locate schema
            String rowSource = table.getAs(SOURCE);
            String rowTable = table.getAs(TABLE);

            // TODO - review this - casting could throw
            StructType schema = (StructType) SourceReferenceService.getSchema(rowSource, rowTable);

            val tableName = SourceReferenceService.getTable(rowSource, rowTable);
            val sourceName = SourceReferenceService.getSource(rowSource, rowTable);
            // TODO - fix this (varargs?)
            val tablePath = getTablePath(structuredS3Path, sourceName, tableName, "");

            // TODO - add a config key for violations path so we can build the correct path here.
            val validationFailedViolationPath = getTablePath(structuredS3Path, "violations", sourceName, tableName);
            val missingSchemaViolationPath = getTablePath(structuredS3Path, "violations", rowSource, rowTable);

            // Filter records on table name in metadata
            // TODO - is this adequate or should we parse out the metadata fields into columns first?
            // TODO - filter on load too
            val dataFrameForTable = dataFrame
                .filter(col("metadata").ilike("%" + rowSource + "." + rowTable + "%"));

            logger.info("Processing {} records for {}/{}",
                dataFrameForTable.count(),
                rowSource,
                rowTable
            );

            if (schema == null) handleNoSchemaFound(dataFrameForTable, rowSource, rowTable, missingSchemaViolationPath);
            else {
                val validatedDataFrame = validateJsonData(dataFrameForTable, schema, sourceName, tableName);
                handleValidRecords(validatedDataFrame, tablePath);
                handleInValidRecords(validatedDataFrame, sourceName, tableName, validationFailedViolationPath);
            }
        });

        logger.info("Processed data frame with {} rows in {}ms",
            dataFrame.count(),
            System.currentTimeMillis() - startTime
        );
    }

    private Dataset<Row> validateJsonData(Dataset<Row> dataFrame, StructType schema, String source, String table) {
        logger.info("Validating data against schema: {}/{}", source, table);

        val jsonValidator = JsonValidator.createAndRegister(schema, dataFrame.sparkSession(), source, table);

        return dataFrame
            .select(col("data"), col("metadata"))
            .withColumn("parsedData", from_json(col("data"), schema))
            .withColumn("valid", jsonValidator.apply(col("data"), to_json(col("parsedData"))));
    }

    private void handleValidRecords(Dataset<Row> dataFrame, String destinationPath) {
        val validRecords = dataFrame
            .select(col("parsedData"), col("valid"))
            .filter(col("valid").equalTo(true))
            .select("parsedData.*");

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
            .select(col("data"), col("metadata"), col("valid"))
            .filter(col("valid").equalTo(false))
            .withColumn("error", lit(errorString))
            .drop(col("valid"));

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

    private void handleNoSchemaFound(Dataset<Row> dataFrame, String source, String table, String destinationPath) {
        logger.error("Structured Zone Violation - No schema found for {}/{} - writing {} records",
            source,
            table,
            dataFrame.count()
        );

        dataFrame
            .select(col("data"), col("metadata"))
            .withColumn("error", lit(String.format("Schema does not exist for %s/%s", source, table)))
            .write()
            .mode(SaveMode.Append)
            .option(PATH, destinationPath)
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
