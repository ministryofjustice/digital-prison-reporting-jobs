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
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDC;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_CDC;

/**
 * Encapsulates logic for processing a single micro-batch of CDC events for a single table.
 * You can test detailed cases for a single micro-batch against this class.
 */
@Singleton
public class CdcBatchProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CdcBatchProcessor.class);
    private final ValidationService validationService;
    private final StructuredZoneCDC structuredZone;
    private final CuratedZoneCDC curatedZone;
    private final S3DataProvider dataProvider;

    @Inject
    public CdcBatchProcessor(
            ValidationService validationService,
            StructuredZoneCDC structuredZone,
            CuratedZoneCDC curatedZone,
            S3DataProvider dataProvider) {
        this.validationService = validationService;
        this.structuredZone = structuredZone;
        this.curatedZone = curatedZone;
        this.dataProvider = dataProvider;
    }

    public void processBatch(SourceReference sourceReference, SparkSession spark, Dataset<Row> df, Long batchId) {
        if(!df.isEmpty()) {
            val batchStartTime = System.currentTimeMillis();
            String source = sourceReference.getSource();
            String table = sourceReference.getTable();
            logger.info("Processing batch {} for {}.{}", batchId, source, table);
            StructType inferredSchema = dataProvider.inferSchema(df.sparkSession(), sourceReference.getSource(), sourceReference.getTable());
            val validRows = validationService.handleValidation(spark, df, sourceReference, inferredSchema, STRUCTURED_CDC);
            val latestCDCRecordsByPK = latestRecords(validRows, sourceReference.getPrimaryKey());

            structuredZone.process(spark, latestCDCRecordsByPK, sourceReference);
            curatedZone.process(spark, latestCDCRecordsByPK, sourceReference);
            logger.info("Processing batch {} {}.{} took {}ms", batchId, source, table, System.currentTimeMillis() - batchStartTime);
        } else {
            logger.info("Skipping empty batch");
        }
    }

    @VisibleForTesting
    static Dataset<Row> latestRecords(Dataset<Row> df, SourceReference.PrimaryKey primaryKey) {
        val primaryKeys = JavaConverters
                .asScalaIteratorConverter(primaryKey.getKeyColumnNames().stream().map(functions::col).iterator())
                .asScala()
                .toSeq();
        val window = Window
                .partitionBy(primaryKeys)
                .orderBy(col(TIMESTAMP).desc());

        return df
                .withColumn("row_number", row_number().over(window))
                .where("row_number = 1")
                .drop("row_number");
    }
}
