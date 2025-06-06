package uk.gov.justice.digital.job;

import com.amazonaws.AbortedException;
import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.Job;
import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.exception.NoSchemaNoDataException;
import uk.gov.justice.digital.job.cdc.TableStreamingQuery;
import uk.gov.justice.digital.job.cdc.TableStreamingQueryProvider;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.TableDiscoveryService;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static uk.gov.justice.digital.config.JobProperties.SPARK_JOB_NAME_PROPERTY;

@Singleton
@CommandLine.Command(name = "DataHubCdcJob")
public class DataHubCdcJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubCdcJob.class);

    private final JobArguments arguments;
    private final JobProperties properties;
    private final SparkSessionProvider sparkSessionProvider;
    private final TableStreamingQueryProvider tableStreamingQueryProvider;
    private final TableDiscoveryService tableDiscoveryService;

    @Inject
    public DataHubCdcJob(
            JobArguments arguments,
            JobProperties properties,
            SparkSessionProvider sparkSessionProvider,
            TableStreamingQueryProvider tableStreamingQueryProvider,
            TableDiscoveryService tableDiscoveryService) {
        logger.info("Initializing DataHubCdcJob");
        this.arguments = arguments;
        this.properties = properties;
        this.sparkSessionProvider = sparkSessionProvider;
        this.tableStreamingQueryProvider = tableStreamingQueryProvider;
        this.tableDiscoveryService = tableDiscoveryService;
        logger.info("DataHubCdcJob initialization complete");
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(DataHubCdcJob.class, args);
    }

    @Override
    public void run() {
        try {
            boolean runLocal = System.getProperty(SPARK_JOB_NAME_PROPERTY) == null;
            String checkpointLocation = arguments.getCheckpointLocation();
            if (runLocal) {
                logger.info("Running locally");
                SparkConf sparkConf = new SparkConf().setAppName("DataHubCdcJob local").setMaster("local[*]");
                SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, arguments, properties);
                if (arguments.cleanCdcCheckpoint()) recreateCheckpoint(spark, checkpointLocation);
                runJob(spark);
                waitUntilQueryTerminates(spark);
            } else {
                logger.info("Running in Glue");
                String jobName = properties.getSparkJobName();
                GlueContext glueContext = sparkSessionProvider.createGlueContext(jobName, arguments, properties);
                SparkSession spark = glueContext.getSparkSession();
                if (arguments.cleanCdcCheckpoint()) recreateCheckpoint(spark, checkpointLocation);
                Job.init(jobName, glueContext, arguments.getConfig());
                runJob(spark);
                waitUntilQueryTerminates(spark);
                Job.commit();
            }
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }

    /**
     * The main entry point for testing a streaming application to process all micro-batches continuously for all tables.
     */
    @VisibleForTesting
    List<TableStreamingQuery> runJob(SparkSession spark) {
        logger.info("Initialising Job");
        List<ImmutablePair<String, String>> tablesToProcess = tableDiscoveryService.discoverTablesToProcess();
        List<TableStreamingQuery> streamingQueries = new ArrayList<>();

        if(!tablesToProcess.isEmpty()) {
            for (val tableDetails: tablesToProcess) {
                String inputSchemaName = tableDetails.getLeft();
                String inputTableName = tableDetails.getRight();
                try {
                    TableStreamingQuery streamingQuery = tableStreamingQueryProvider.provide(spark, inputSchemaName, inputTableName);
                    streamingQuery.runQuery();
                    streamingQueries.add(streamingQuery);
                } catch (NoSchemaNoDataException e) {
                    logger.error("No schema and no data for {}.{}. We will skip this table and continue processing the other tables",
                            inputSchemaName, inputTableName, e);
                }

            }
        } else {
            logger.warn("No tables to process");
        }
        logger.info("Job finished");
        return streamingQueries;
    }

    private void waitUntilQueryTerminates(SparkSession spark) {
        try {
            spark.streams().awaitAnyTermination();
        } catch (StreamingQueryException e) {
            if (isAbortedException(e)) {
                logger.info("Job terminated because of Aborted Exception");
                System.exit(0);
            } else {
                logger.error("A streaming query terminated with an Exception", e);
                throw new RuntimeException(e);
            }
        }
    }

    private static void recreateCheckpoint(SparkSession spark, String checkpointLocation) throws IOException {
        logger.info("Deleting checkpoint directory: {}", checkpointLocation);
        val checkpointURI = URI.create(checkpointLocation);
        try (val fileSystem = FileSystem.get(checkpointURI, spark.sparkContext().hadoopConfiguration())) {
            val checkpointPath = new Path(checkpointURI);
            if (fileSystem.exists(checkpointPath)) {
                fileSystem.delete(checkpointPath, true);
            }
        }
        logger.info("Checkpoint directory deleted");
    }

    @NotNull
    private static Boolean isAbortedException(StreamingQueryException e) {
        return ExceptionUtils.getRootCause(e) instanceof AbortedException;
    }
}
