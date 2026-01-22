package uk.gov.justice.digital.job.cdc;

import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecutionException;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.NoSchemaNoDataException;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.service.metrics.BatchMetrics;
import uk.gov.justice.digital.service.metrics.BatchedMetricReportingService;

import javax.inject.Singleton;
import java.util.Optional;

import static java.lang.String.format;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_CDC;

@Singleton
public class TableStreamingQueryProvider {

    private static final Logger logger = LoggerFactory.getLogger(TableStreamingQueryProvider.class);

    private final JobArguments arguments;
    private final S3DataProvider s3DataProvider;
    private final CdcBatchProcessor batchProcessor;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;
    private final BatchedMetricReportingService metricReportingService;

    @Inject
    public TableStreamingQueryProvider(
            JobArguments arguments,
            S3DataProvider s3DataProvider,
            CdcBatchProcessor batchProcessor,
            SourceReferenceService sourceReferenceService,
            ViolationService violationService,
            BatchedMetricReportingService metricReportingService) {
        this.arguments = arguments;
        this.s3DataProvider = s3DataProvider;
        this.batchProcessor = batchProcessor;
        this.sourceReferenceService = sourceReferenceService;
        this.violationService = violationService;
        this.metricReportingService = metricReportingService;
    }

    public TableStreamingQuery provide(SparkSession spark, String inputSourceName, String inputTableName) throws NoSchemaNoDataException {
        // We'll build the streaming query we want here. It might do normal processing, or it might write everything to violations
        Optional<SourceReference> maybeSourceReference = sourceReferenceService.getSourceReference(inputSourceName, inputTableName);
        if (maybeSourceReference.isPresent()) {
            SourceReference sourceReference = maybeSourceReference.get();
            logger.info("{}/{} looks good so we will do normal batch processing", inputSourceName, inputTableName);
            return standardProcessingQuery(spark, inputSourceName, inputTableName, sourceReference);
        } else {
            logger.warn("{}/{} has no source reference so we will write all data to violations until the stream restarts", inputSourceName, inputTableName);
            return noSchemaFoundQuery(spark, inputSourceName, inputTableName);
        }
    }

    @VisibleForTesting
    TableStreamingQuery standardProcessingQuery(
            SparkSession spark,
            String inputSourceName,
            String inputTableName,
            SourceReference sourceReference
    ) {

        Dataset<Row> sourceData = s3DataProvider.getStreamingSourceData(spark, sourceReference);

        VoidFunction2<Dataset<Row>, Long> batchProcessingFunc = metricReportingService.withBatchedMetrics(batchMetrics -> {

            VoidFunction2<Dataset<Row>, Long> processBatch = (df, batchId) ->
                    batchProcessor.processBatch(sourceReference, spark, batchMetrics, df, batchId);

            return withIncompatibleSchemaHandling(inputSourceName, inputTableName, batchMetrics, processBatch);
        });

        return new TableStreamingQuery(
                inputSourceName,
                inputTableName,
                arguments.getCheckpointLocation(),
                arguments.getCdcTriggerIntervalSeconds(),
                sourceData,
                batchProcessingFunc
        );
    }

    @VisibleForTesting
    TableStreamingQuery noSchemaFoundQuery(SparkSession spark, String inputSourceName, String inputTableName) throws NoSchemaNoDataException {
        Dataset<Row> sourceData = s3DataProvider.getStreamingSourceDataWithSchemaInference(spark, inputSourceName, inputTableName);

        VoidFunction2<Dataset<Row>, Long> batchProcessingFunc = metricReportingService.withBatchedMetrics(batchMetrics -> {

            VoidFunction2<Dataset<Row>, Long> violateEverything = (df, batchId) ->
                    violationService.handleNoSchemaFound(spark, batchMetrics, df, inputSourceName, inputTableName, STRUCTURED_CDC);

            return withIncompatibleSchemaHandling(inputSourceName, inputTableName, batchMetrics, violateEverything);
        });

        return new TableStreamingQuery(
                inputSourceName,
                inputTableName,
                arguments.getCheckpointLocation(),
                arguments.getCdcTriggerIntervalSeconds(),
                sourceData,
                batchProcessingFunc
        );
    }


    @VisibleForTesting
        // Add handling of incompatible schemas to the batch processing function.
        // If files use a schema which cannot be read using the configured input schema then write to violations and continue.
    VoidFunction2<Dataset<Row>, Long> withIncompatibleSchemaHandling(String source, String table, BatchMetrics batchMetrics, VoidFunction2<Dataset<Row>, Long> originalFunc) {
        return (df, batchId) -> {
            try {
                originalFunc.call(df, batchId);
            } catch (SparkException e) {
                // We only want to handle a very specific Exception (wrapped in two others) here
                if (e.getCause() instanceof QueryExecutionException &&
                        e.getCause().getCause() instanceof SchemaColumnConvertNotSupportedException) {
                    SchemaColumnConvertNotSupportedException cause =
                            (SchemaColumnConvertNotSupportedException) e.getCause().getCause();
                    String msg = format("Violation - some of the input data had incompatible types for column %s. Tried to use %s but found %s",
                            cause.getColumn(), cause.getLogicalType(), cause.getPhysicalType());
                    logger.warn(msg, e);
                    violationService.writeCdcDataToViolations(df.sparkSession(), batchMetrics, source, table, msg);
                } else {
                    throw e;
                }
            }
        };
    }
}
