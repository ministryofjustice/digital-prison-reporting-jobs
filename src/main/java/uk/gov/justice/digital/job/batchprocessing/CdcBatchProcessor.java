package uk.gov.justice.digital.job.batchprocessing;

import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.CDC;

/**
 * Encapsulates logic for processing a single micro-batch of CDC events for a single table.
 * You can test detailed cases for a single micro-batch against this class.
 */
@Singleton
public class CdcBatchProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CdcBatchProcessor.class);
    private final ViolationService violationService;
    private final ValidationService validationService;
    private final DataStorageService storage;

    @Inject
    public CdcBatchProcessor(
            ViolationService violationService,
            ValidationService validationService,
            DataStorageService storage) {
        this.violationService = violationService;
        this.validationService = validationService;
        this.storage = storage;
    }

    public void processBatch(SourceReference sourceReference, SparkSession spark, Dataset<Row> df, Long batchId, String structuredTablePath, String curatedTablePath) {
        val batchStartTime = System.currentTimeMillis();
        logger.info("Processing batch {} for {}.{}", batchId, sourceReference.getSource(), sourceReference.getTable());
        val primaryKey = sourceReference.getPrimaryKey();

        val validRows = validationService.handleValidation(spark, df, sourceReference);
        val latestCDCRecordsByPK = latestRecords(validRows, primaryKey);
        try {
            storage.mergeRecordsCdc(spark, structuredTablePath, latestCDCRecordsByPK, primaryKey);
            storage.mergeRecordsCdc(spark, curatedTablePath, latestCDCRecordsByPK, primaryKey);
            // Manifests are only required for the curated Zone
            storage.updateDeltaManifestForTable(spark, curatedTablePath);
        } catch (DataStorageRetriesExhaustedException e) {
            violationService.handleRetriesExhausted(spark, latestCDCRecordsByPK, sourceReference.getSource(), sourceReference.getTable(), e, CDC);
        }
        logger.info("Processing batch {} {}.{} took {}ms", batchId, sourceReference.getSource(), sourceReference.getTable(), System.currentTimeMillis() - batchStartTime);
    }

    @VisibleForTesting
    static Dataset<Row> latestRecords(Dataset<Row> df, SourceReference.PrimaryKey primaryKey) {
        val primaryKeys = JavaConverters
                .asScalaIteratorConverter(primaryKey.getKeyColumnNames().stream().map(functions::col).iterator())
                .asScala()
                .toSeq();
        val window = Window
                .partitionBy(primaryKeys)
                // TODO Move TIMESTAMP where it makes sense
                .orderBy(col(TIMESTAMP).desc());

        return df
                .withColumn("row_number", row_number().over(window))
                .where("row_number = 1")
                .drop("row_number");
    }
}
