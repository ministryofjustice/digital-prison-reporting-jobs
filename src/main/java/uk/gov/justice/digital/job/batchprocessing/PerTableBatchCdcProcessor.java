package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.CuratedCDCService;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.StructuredCDCService;
import uk.gov.justice.digital.service.ViolationService;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.SOURCE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TABLE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.CURATED_CDC;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_CDC;

@Singleton
public class PerTableBatchCdcProcessor {

    private static final Logger logger = LoggerFactory.getLogger(PerTableBatchCdcProcessor.class);

    private final JobArguments jobArguments;
    private final ViolationService violationService;
    private final DataStorageService storage;
    private final StructuredCDCService structured;
    private final CuratedCDCService curated;

    @Inject
    public PerTableBatchCdcProcessor(
            JobArguments jobArguments,
            ViolationService violationService,
            DataStorageService storage,
            StructuredCDCService structured,
            CuratedCDCService curated) {
        this.jobArguments = jobArguments;
        this.violationService = violationService;
        this.storage = storage;
        this.structured = structured;
        this.curated = curated;
    }

    public void processTable(SparkSession spark, Dataset<Row> dataFrameForTable, SourceReference sourceReference) throws DataStorageException {
        val primaryKey = sourceReference.getPrimaryKey();
        if (dataFrameIsValid(dataFrameForTable, sourceReference)) {
            val latestCDCRecordsByPK = latestRecords(dataFrameForTable.drop(SOURCE, TABLE), primaryKey);
            try {
                structured.applyUpdates(spark, latestCDCRecordsByPK, sourceReference);
                curated.applyUpdates(spark, latestCDCRecordsByPK, sourceReference);
            } catch (DataStorageRetriesExhaustedException e) {
                violationService.handleRetriesExhausted(spark, dataFrameForTable, sourceReference.getSource(), sourceReference.getTable(), e, STRUCTURED_CDC);
            }
        } else {
            writeInvalid(dataFrameForTable, sourceReference, spark);
        }
    }

    private static boolean dataFrameIsValid(Dataset<Row> dataFrame, SourceReference sourceReference) {
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

    private static Dataset<Row> latestRecords(Dataset<Row> df, SourceReference.PrimaryKey primaryKey) {
        val primaryKeys = JavaConverters
                .asScalaIteratorConverter(primaryKey.keys.stream().map(functions::col).iterator())
                .asScala()
                .toSeq();
        val window = Window.partitionBy(primaryKeys)
                .orderBy(
                        unix_timestamp(
                                col(TIMESTAMP),
                                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                        ).cast(TimestampType).desc()
                );

        return df
                .withColumn("rank", rank().over(window))
                .where("rank = 1")
                .drop("rank");
    }

    private void writeInvalid(Dataset<Row> dataFrame, SourceReference sourceReference, SparkSession spark) throws DataStorageException {
        // todo move to violations service
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        val violationsPath = jobArguments.getViolationsS3Path();
        String validationFailedViolationPath = createValidatedPath(violationsPath, sourceName, tableName);
        val errorPrefix = String.format("Record does not match schema %s/%s: ", sourceName, tableName);

        val invalidRecords = dataFrame.withColumn(ERROR, concat(lit(errorPrefix), lit(col(ERROR))));

        logger.warn("Violation - Records failed schema validation for source {}, table {}", sourceName, tableName);
        logger.info("Appending {} records to deltalake table: {}", invalidRecords.count(), validationFailedViolationPath);
        storage.append(validationFailedViolationPath, invalidRecords.drop(OPERATION));

        logger.info("Append completed successfully");
        storage.updateDeltaManifestForTable(spark, validationFailedViolationPath);
    }
}
