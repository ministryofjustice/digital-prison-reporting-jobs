package uk.gov.justice.digital.zone;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.*;

@Singleton
public class StructuredZone implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZone.class);

    // TODO - refactor this out into a separate validator class
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // TODO - this duplicates the constants in RawZone
    private static final String LOAD = "load";
    private static final String SOURCE = "source";
    private static final String TABLE = "table";
    private static final String OPERATION = "operation";
    private static final String PATH = "path";

    private final String structuredS3Path;

    @Inject
    public StructuredZone(JobParameters jobParameters) {
        this.structuredS3Path = jobParameters.getStructuredS3Path()
            .orElseThrow(() -> new IllegalStateException(
                "structured s3 path now set - unable to create StructuredZone instance"
            ));
    }

    @Override
    public void process(Dataset<Row> dataFrame) {

        logger.info("Processing data frame with " + dataFrame.count() + " rows");

        val startTime = System.currentTimeMillis();

        uniqueTablesForLoad(dataFrame).forEach((table) -> {
            logger.info("Processing table: {}", table);

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
            val dataFrameForTable = dataFrame
                .filter(col("metadata").contains(String.join(".", rowSource, rowTable)));

            if (schema != null) {

                logger.info("Validating data against schema: {}/{}", tableName, sourceName);

                val udfName = "jsonValidatorFor" + tableName + sourceName;
                val jsonValidator = dataFrameForTable
                    .sparkSession()
                    .udf()
                    .register(udfName, createJsonValidator(schema));

                val validatedData = dataFrameForTable
                    .select(col("data"), col("metadata"))
                    .withColumn("parsedData", from_json(col("data"), schema))
                    .withColumn("valid", jsonValidator.apply(col("data"), to_json(col("parsedData"))));

                // Write valid records
                validatedData
                    .select(col("parsedData"), col("valid"))
                    .filter(col("valid").equalTo(true))
                    .select("parsedData.*")
                    .write()
                    .mode(SaveMode.Append)
                    .option(PATH, tablePath)
                    .format("delta")
                    .save();

                val errorString = String.format(
                    "Record does not match schema %s/%s",
                    rowSource,
                    rowTable
                );

                // Write invalid records where schema validation failed
                validatedData
                    .select(col("data"), col("metadata"), col("valid"))
                    .filter(col("valid").equalTo(false))
                    .withColumn("error", lit(errorString))
                    .drop(col("valid"))
                    .write()
                    .mode(SaveMode.Append)
                    .option(PATH, validationFailedViolationPath)
                    .format("delta")
                    .save();
            }
            else {
                // Write to violation bucket
                logger.error("No schema found for {}/{} - writing to violations", rowSource, rowTable);

                val errorString = String.format(
                    "Schema does not exist for %s/%s",
                    rowSource,
                    rowTable
                );

                dataFrameForTable
                    .select(col("data"), col("metadata"))
                    .withColumn("error", lit(errorString))
                    .write()
                    .mode(SaveMode.Append)
                    .option(PATH, missingSchemaViolationPath)
                    .format("delta")
                    .save();
            }

            logger.info("Processing completed.");
        });

        logger.info("Processed data frame with {} rows in {}ms",
            dataFrame.count(),
            System.currentTimeMillis() - startTime
        );
    }

    // TODO - duplicated from RawZone
    private List<Row> uniqueTablesForLoad(Dataset<Row> dataFrame) {
        return dataFrame
            .filter(col(OPERATION).isin(LOAD))
            .select(TABLE, SOURCE, OPERATION)
            .distinct()
            .collectAsList();
    }

    private static UserDefinedFunction createJsonValidator(StructType schema) {
        return udf(
            (UDF2<String, String, Boolean>) (String originalJson, String parsedJson) -> {
                return validateJson(originalJson, parsedJson, schema);
            }, DataTypes.BooleanType);
    }

    public static boolean validateJson(
        String originalJson,
        String parsedJson,
        StructType schema
    ) throws JsonProcessingException {

        val originalData = objectMapper.readTree(originalJson);
        val parsedData = objectMapper.readTree(parsedJson);

        // Check that the original and parsed json trees match. If there are discrepancies then the initial parse must
        // have encountered an invalid field and set it to null (e.g. got string when int expected - this will be set
        // to null in the DataFrame.
        if (!originalData.equals(parsedData)) return false;


        // Verify that all notNull fields have a value set.
        for (StructField structField : schema.fields()) {
            // Skip fields that are declared nullable in the table schema.
            if (structField.nullable()) continue;

            val jsonField = Optional.ofNullable(originalData.get(structField.name()));

            val jsonFieldIsNull = jsonField
                .map(JsonNode::isNull)  // Field present in JSON but with no value
                .orElse(true);         // If no field found then it's null by default.

            // Verify that the current JSON field in the original data set is not null.
            if (jsonFieldIsNull) return false;
        }

        return true;
    }

}
