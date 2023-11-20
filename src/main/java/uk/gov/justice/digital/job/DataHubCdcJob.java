package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.Job;
import com.google.common.annotations.VisibleForTesting;
import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.job.cdc.TableStreamingQuery;
import uk.gov.justice.digital.job.cdc.TableStreamingQueryProvider;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.TableDiscoveryService;

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
        logger.info("Job started");
        PicocliRunner.run(DataHubCdcJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        boolean runLocal = System.getProperty(SPARK_JOB_NAME_PROPERTY) == null;
        if(runLocal) {
            logger.info("Running locally");
            SparkConf sparkConf = new SparkConf().setAppName("DataHubCdcJob local").setMaster("local[*]");
            SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, arguments.getLogLevel());
            runJob(spark);
            waitUntilQueryTerminates(spark);
        } else {
            logger.info("Running in Glue");
            String jobName = properties.getSparkJobName();
            GlueContext glueContext = sparkSessionProvider.createGlueContext(jobName, arguments.getLogLevel());
            SparkSession spark = glueContext.getSparkSession();
            Job.init(jobName, glueContext, arguments.getConfig());
            runJob(spark);
            waitUntilQueryTerminates(spark);
            Job.commit();
        }
    }

    /**
     * The main entry point for starting a streaming application to process all micro-batches continuously for all tables.
     */
    @VisibleForTesting
    List<TableStreamingQuery> runJob(SparkSession spark) {
        logger.info("Initialising Job");
        List<ImmutablePair<String, String>> tablesToProcess = tableDiscoveryService.discoverTablesToProcess();
        List<TableStreamingQuery> streamingQueries = new ArrayList<>();

        if(!tablesToProcess.isEmpty()) {
            tablesToProcess.forEach(tableDetails -> {
                String inputSchemaName = tableDetails.getLeft();
                String inputTableName = tableDetails.getRight();
                TableStreamingQuery streamingQuery = tableStreamingQueryProvider.provide(inputSchemaName, inputTableName);
                streamingQuery.runQuery(spark);
                streamingQueries.add(streamingQuery);
            });
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
            logger.error("A streaming query terminated with an Exception", e);
            throw new RuntimeException(e);
        }
    }
}
