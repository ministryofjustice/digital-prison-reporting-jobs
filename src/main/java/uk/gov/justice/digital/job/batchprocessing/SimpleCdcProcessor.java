package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
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
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.DomainService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;

import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.SOURCE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TABLE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;

/**
 * Responsible for processing batches of DMS records.
 * Dependencies which rely on the SparkSession are manually injected to ensure spark contexts/sessions/etc.
 * are created just once with a common configuration.
 */
@Singleton
public class SimpleCdcProcessor {

    private static final Logger logger = LoggerFactory.getLogger(SimpleCdcProcessor.class);

    private final JobArguments jobArguments;
    private final CuratedZoneCDC curatedZoneCdc;
    private final DomainService domainService;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;
    private final DataStorageService storage;

    @Inject
    public SimpleCdcProcessor(
            JobArguments jobArguments,
            CuratedZoneCDC curatedZoneCdc,
            DomainService domainService,
            SourceReferenceService sourceReferenceService,
            ViolationService violationService,
            DataStorageService storage
    ) {
        logger.info("Initializing S3BatchProcessor");
        this.jobArguments = jobArguments;
        this.curatedZoneCdc = curatedZoneCdc;
        this.sourceReferenceService = sourceReferenceService;
        this.domainService = domainService;
        this.violationService = violationService;
        this.storage = storage;
        logger.info("S3BatchProcessor initialization complete");
    }

    public void processCDC(SparkSession spark, Dataset<Row> dataFrame) {
        logger.info("Processing CDC batch");
        val cdcBatchStartTime = System.currentTimeMillis();
        dataFrame.persist();

        getTablesInBatch(dataFrame).forEach(tableInfo -> {
            String sourceName = tableInfo.getAs(SOURCE);
            String tableName = tableInfo.getAs(TABLE);

            logger.info("Processing records {}/{}", sourceName, tableName);
            val startTime = System.currentTimeMillis();

            try {
                val optionalSourceReference = sourceReferenceService.getSourceReference(sourceName, tableName);

                if (optionalSourceReference.isPresent()) {
                    val dataFrameForTable = extractDataFrameForSourceTable(dataFrame, tableInfo);
                    val sourceReference = optionalSourceReference.get();
                    if (dataFrameIsValid(dataFrameForTable, sourceReference)) {
                        val structuredPath = jobArguments.getStructuredS3Path();
                        val structuredTablePath = createValidatedPath(structuredPath, sourceName, tableName);
                        val dataFrameWithDataFieldsOnly = dataFrame.drop(SOURCE, TABLE);

//                        try {
                            // TODO Get latest row for every PK
                            // TODO DataStorage merge
//                    handleValidRecords(spark, dataFrameWithDataFieldsOnly, tablePath, sourceReference.getPrimaryKey());
                            storage.updateDeltaManifestForTable(spark, structuredTablePath);
//                        } catch (DataStorageRetriesExhaustedException e) {
//                            violationService.handleRetriesExhausted(spark, dataFrameForTable, sourceReference.getSource(), sourceReference.getTable(), e, STRUCTURED_CDC);
//                        }

//                    val curatedCdcDataFrame = curatedZoneCdc.process(spark, structuredIncrementalDataFrame, sourceReference);

//                    if (!curatedCdcDataFrame.isEmpty()) domainService
//                            .refreshDomainUsingDataFrame(
//                                    spark,
//                                    curatedCdcDataFrame,
//                                    sourceReference.getSource(),
//                                    sourceReference.getTable()
//                            );
                    } else {
                        writeInvalid(dataFrameForTable, sourceReference, spark);
                    }
                } else {
                    violationService.handleNoSchemaFound(spark, dataFrame, sourceName, tableName);
                }

                logger.info("Processed records {}/{} in {}ms",
                        sourceName,
                        tableName,
                        System.currentTimeMillis() - startTime
                );
            } catch (Exception e) {
                logger.error("Caught unexpected exception", e);
                throw new RuntimeException("Caught unexpected exception", e);
            }
        });
        dataFrame.unpersist();
        logger.info("Processed CDC batch in {}ms", System.currentTimeMillis() - cdcBatchStartTime);
    }

    private List<Row> getTablesInBatch(Dataset<Row> dataFrame) {
        return dataFrame
                .select(TABLE, SOURCE, OPERATION)
                .dropDuplicates(TABLE, SOURCE)
                .collectAsList();
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

    private Dataset<Row> extractDataFrameForSourceTable(Dataset<Row> dataFrame, Row row) {
        final String source = row.getAs(SOURCE);
        final String table = row.getAs(TABLE);
        return dataFrame
                .filter(col(SOURCE).equalTo(source).and(col(TABLE).equalTo(table)))
                .orderBy(col(TIMESTAMP));
    }

}
