package uk.gov.justice.digital.job.cdc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.ViolationService;

import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;

// todo tests
public class SchemaViolationTableStreamingQuery implements TableStreamingQuery {

    private static final Logger logger = LoggerFactory.getLogger(SchemaViolationTableStreamingQuery.class);

    private final String sourceName;
    private final String tableName;
    private final JobArguments arguments;
    private final S3DataProvider dataProvider;
    private final ViolationService violationService;

    private StreamingQuery query;

    public SchemaViolationTableStreamingQuery(
            String sourceName,
            String tableName,
            JobArguments arguments,
            S3DataProvider dataProvider,
            ViolationService violationService
    ) {
        this.sourceName = sourceName;
        this.tableName = tableName;
        this.arguments = arguments;
        this.dataProvider = dataProvider;
        this.violationService = violationService;
    }


    @Override
    public StreamingQuery runQuery(SparkSession spark) {
        logger.info("Initialising schema violation per batch processing for {}/{}", sourceName, tableName);
        String queryName = format("Datahub CDC Schema Violations %s.%s", sourceName, tableName);
        String queryCheckpointPath = format("%sDataHubCdcJob/%s", ensureEndsWithSlash(arguments.getCheckpointLocation()), queryName);

        logger.info("Initialising query {} with checkpoint path {}", queryName, queryCheckpointPath);
        Dataset<Row> sourceDf = dataProvider.getSourceDataStreamingWithSchemaInference(spark, sourceName, tableName);
        try {
            query = sourceDf
                    .writeStream()
                    .queryName(queryName)
                    .format("delta")
                    .foreachBatch((df, batchId) -> {
                       violationService.handleNoSchemaFoundS3(spark, df, sourceName, tableName);
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

    @Override
    public void stopQuery() throws TimeoutException {
        query.stop();
    }
}
