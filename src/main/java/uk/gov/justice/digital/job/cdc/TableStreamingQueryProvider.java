package uk.gov.justice.digital.job.cdc;

import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.SchemaMismatchException;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class TableStreamingQueryProvider {

    private static final Logger logger = LoggerFactory.getLogger(TableStreamingQueryProvider.class);

    private final JobArguments arguments;
    private final S3DataProvider s3DataProvider;
    private final CdcBatchProcessor batchProcessor;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;

    @Inject
    public TableStreamingQueryProvider(
            JobArguments arguments,
            S3DataProvider s3DataProvider,
            CdcBatchProcessor batchProcessor,
            SourceReferenceService sourceReferenceService,
            ViolationService violationService) {
        this.arguments = arguments;
        this.s3DataProvider = s3DataProvider;
        this.batchProcessor = batchProcessor;
        this.sourceReferenceService = sourceReferenceService;
        this.violationService = violationService;
    }

    public TableStreamingQuery create(SparkSession spark, String inputSourceName, String inputTableName) {
        // We'll build the streaming query we want here. It might do normal processing, or it might write everything to violations
        Optional<SourceReference> maybeSourceReference = sourceReferenceService.getSourceReference(inputSourceName, inputTableName);
        if(!maybeSourceReference.isPresent()) {
            logger.warn("{}/{} has no source reference so we will write all data to violations until the stream restarts", inputSourceName, inputTableName);
            return noSchemaFoundQuery(spark, inputSourceName, inputTableName);
        } else {
            SourceReference sourceReference = maybeSourceReference.get();
            try {
                logger.info("{}/{} looks good so we will do normal batch processing", inputSourceName, inputTableName);
                return standardProcessingQuery(spark, inputSourceName, inputTableName, sourceReference);
            } catch (SchemaMismatchException e) {
                logger.warn("{}/{} has a schema mismatch so we will write all data to violations until the stream restarts", inputSourceName, inputTableName);
                return schemaMismatchQuery(spark, inputSourceName, inputTableName, sourceReference);
            }
        }
    }

    @VisibleForTesting
    TableStreamingQuery standardProcessingQuery(SparkSession spark, String inputSourceName, String inputTableName, SourceReference sourceReference) throws SchemaMismatchException {
        Dataset<Row> sourceData = s3DataProvider.getStreamingSourceData(spark, sourceReference);
        VoidFunction2<Dataset<Row>, Long> batchProcessingFunc = (df, batchId) -> batchProcessor.processBatch(sourceReference, spark, df, batchId);
        return new TableStreamingQuery(inputSourceName, inputTableName, arguments.getCheckpointLocation(), sourceData, batchProcessingFunc);
    }

    @VisibleForTesting
    TableStreamingQuery noSchemaFoundQuery(SparkSession spark, String inputSourceName, String inputTableName) {
        Dataset<Row> sourceData = s3DataProvider.getStreamingSourceDataWithSchemaInference(spark, inputSourceName, inputTableName);
        VoidFunction2<Dataset<Row>, Long> batchProcessingFunc = (df, batchId) -> violationService.handleNoSchemaFoundS3(spark, df, inputSourceName, inputTableName);
        return new TableStreamingQuery(inputSourceName, inputTableName, arguments.getCheckpointLocation(), sourceData, batchProcessingFunc);
    }

    @VisibleForTesting
    TableStreamingQuery schemaMismatchQuery(SparkSession spark, String inputSourceName, String inputTableName, SourceReference sourceReference) {
        Dataset<Row> sourceData = s3DataProvider.getStreamingSourceDataWithSchemaInference(spark, sourceReference.getSource(), sourceReference.getTable());
        VoidFunction2<Dataset<Row>, Long> batchProcessingFunc = (df, batchId) -> violationService.handleInvalidSchema(spark, df, sourceReference.getSource(), sourceReference.getTable());
        return new TableStreamingQuery(inputSourceName, inputTableName, arguments.getCheckpointLocation(), sourceData, batchProcessingFunc);
    }
}
