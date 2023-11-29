package uk.gov.justice.digital.job.cdc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;

import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;

/**
 * Encapsulates logic for standard processing of a stream of micro-batches of CDC events for a single table.
 * You can test behaviour across multiple batches by testing against this class.
 */
public class ProcessingTableStreamingQuery implements TableStreamingQuery {

    private static final Logger logger = LoggerFactory.getLogger(ProcessingTableStreamingQuery.class);

    private final JobArguments arguments;
    private final S3DataProvider s3DataProvider;
    private final CdcBatchProcessor batchProcessor;
    private final SourceReference sourceReference;

    private StreamingQuery query;

    public ProcessingTableStreamingQuery(
            JobArguments arguments,
            S3DataProvider dataProvider,
            CdcBatchProcessor batchProcessor,
            SourceReference sourceReference
    ) {
        this.arguments = arguments;
        this.s3DataProvider = dataProvider;
        this.batchProcessor = batchProcessor;
        this.sourceReference = sourceReference;
    }


    public StreamingQuery runQuery(SparkSession spark) {

        logger.info("Initialising per batch processing for {}/{}", sourceReference.getSource(), sourceReference.getTable());

        String queryName = format("Datahub CDC %s.%s", sourceReference.getSource(), sourceReference.getTable());
        String queryCheckpointPath = format("%sDataHubCdcJob/%s", ensureEndsWithSlash(arguments.getCheckpointLocation()), queryName);

        logger.info("Initialising query {} with checkpoint path {}", queryName, queryCheckpointPath);
        Dataset<Row> sourceDf = s3DataProvider.getSourceDataStreaming(spark, sourceReference);
        try {
            query = sourceDf
                    .writeStream()
                    .queryName(queryName)
                    .format("delta")
                    .foreachBatch((df, batchId) -> {
                        batchProcessor.processBatch(sourceReference, spark, df, batchId);
                    })
                    .outputMode("update")
                    .option("checkpointLocation", queryCheckpointPath)
                    .start();
            logger.info("Started query {}", queryName);
            return query;
        } catch (TimeoutException e) {
            logger.error("Encountered TimeoutException when running streaming query start", e);
            throw new RuntimeException(e);
        }
    }

    public void stopQuery() throws TimeoutException {
        query.stop();
    }
}
