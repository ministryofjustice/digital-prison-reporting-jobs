package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoadS3;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoadS3;

import javax.inject.Singleton;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ShortOperationCode.Insert;

/**
 * Responsible for processing batches of DMS records.
 * Dependencies which rely on the SparkSession are manually injected to ensure spark contexts/sessions/etc.
 * are created just once with a common configuration.
 */
@Singleton
public class S3BatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(S3BatchProcessor.class);

    private final StructuredZoneLoadS3 structuredZoneLoad;
    private final CuratedZoneLoadS3 curatedZoneLoad;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;

    @Inject
    public S3BatchProcessor(
            StructuredZoneLoadS3 structuredZoneLoad,
            CuratedZoneLoadS3 curatedZoneLoad,
            SourceReferenceService sourceReferenceService,
            ViolationService violationService
    ) {
        logger.info("Initializing S3BatchProcessor");
        this.structuredZoneLoad = structuredZoneLoad;
        this.curatedZoneLoad = curatedZoneLoad;
        this.sourceReferenceService = sourceReferenceService;
        this.violationService = violationService;
        logger.info("S3BatchProcessor initialization complete");
    }

    public void processBatch(SparkSession spark, String sourceName, String tableName, Dataset<Row> dataFrame) {
        logger.info("Processing records {}/{}", sourceName, tableName);

        val startTime = System.currentTimeMillis();
        dataFrame.persist();
        try {
            withValidations(spark, sourceName, tableName, dataFrame, (validatedDf, sourceReference) -> {
                val transformedDf = dataFrame.transform(S3BatchProcessor::loadDataTransform);
                val structuredLoadDataFrame = structuredZoneLoad.process(spark, transformedDf, sourceReference);
                curatedZoneLoad.process(spark, structuredLoadDataFrame, sourceReference);
            });
        } catch (Exception e) {
            logger.error("Caught unexpected exception", e);
            throw new RuntimeException("Caught unexpected exception", e);
        }
        dataFrame.unpersist();

        logger.info("Processed records {}/{} in {}ms",
                sourceName,
                tableName,
                System.currentTimeMillis() - startTime
        );
    }

    private static Dataset<Row> loadDataTransform(Dataset<Row> dataFrame) {
        return dataFrame.where(col(OPERATION).equalTo(Insert.getName()));
    }

    @FunctionalInterface
    private interface ValidatedDataframeHandler {
        void apply(Dataset<Row> validDf, SourceReference sourceReference) throws DataStorageException;
    }

    private void withValidations(SparkSession spark, String sourceName, String tableName, Dataset<Row> dataFrame, ValidatedDataframeHandler validatedDfHandler) throws DataStorageException {
        val optionalSourceReference = sourceReferenceService.getSourceReference(sourceName, tableName);

        if (optionalSourceReference.isPresent()) {
            val sourceReference = optionalSourceReference.get();
            if (violationService.dataFrameSchemaIsValid(dataFrame.schema(), sourceReference)) {
                validatedDfHandler.apply(dataFrame, sourceReference);
            } else {
                val source = sourceReference.getSource();
                val table = sourceReference.getTable();
                violationService.handleInvalidSchema(spark, dataFrame, source, table);
            }
        } else {
            violationService.handleNoSchemaFound(spark, dataFrame, sourceName, tableName);
        }
    }



}
