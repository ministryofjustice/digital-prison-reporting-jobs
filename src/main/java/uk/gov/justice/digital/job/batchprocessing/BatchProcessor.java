package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreServiceI;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

import javax.inject.Singleton;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;

/**
 * Responsible for processing batches of DMS records.
 * Dependencies which rely on the SparkSession are manually injected to ensure spark contexts/sessions/etc.
 * are created just once with a common configuration.
 */
@Singleton
public class BatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    private final StructuredZoneLoad structuredZoneLoad;
    private final CuratedZoneLoad curatedZoneLoad;
    private final ValidationService validationService;
    private final OperationalDataStoreServiceI operationalDataStoreService;

    @Inject
    public BatchProcessor(
            StructuredZoneLoad structuredZoneLoad,
            CuratedZoneLoad curatedZoneLoad,
            ValidationService validationService,
            OperationalDataStoreServiceI operationalDataStoreService) {
        logger.info("Initializing S3BatchProcessor");
        this.structuredZoneLoad = structuredZoneLoad;
        this.curatedZoneLoad = curatedZoneLoad;
        this.validationService = validationService;
        this.operationalDataStoreService = operationalDataStoreService;
        logger.info("S3BatchProcessor initialization complete");
    }

    @SuppressWarnings({"java:S2139", "java:S112"})
    public void processBatch(SparkSession spark, SourceReference sourceReference, Dataset<Row> dataFrame) {
        if(!dataFrame.isEmpty()) {
            String sourceName = sourceReference.getSource();
            String tableName = sourceReference.getTable();
            logger.info("Processing records {}/{}", sourceName, tableName);

            val startTime = System.currentTimeMillis();
            dataFrame.persist();
            val filteredDf = dataFrame.where(col(OPERATION).equalTo(Insert.getName()));
            StructType inferredSchema = filteredDf.schema();
            val validRows = validationService.handleValidation(spark, filteredDf, sourceReference, inferredSchema, STRUCTURED_LOAD);
            val structuredLoadDf = structuredZoneLoad.process(spark, validRows, sourceReference);
            val curatedLoadDf = curatedZoneLoad.process(spark, structuredLoadDf, sourceReference);
            operationalDataStoreService.storeBatchData(curatedLoadDf, sourceReference);
            dataFrame.unpersist();

            logger.info("Processed records {}/{} in {}ms",
                    sourceName,
                    tableName,
                    System.currentTimeMillis() - startTime
            );
        } else {
            logger.info("Skipping empty batch");
        }
    }
}
