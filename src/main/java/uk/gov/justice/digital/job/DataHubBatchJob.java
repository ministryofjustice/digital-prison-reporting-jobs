package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.job.batchprocessing.S3BatchProcessor;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import java.net.URI;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.Load;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.*;

@Singleton
@CommandLine.Command(name = "DataHubBatchJob")
public class DataHubBatchJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubBatchJob.class);

    private final JobArguments arguments;
    private final SparkSessionProvider sparkSessionProvider;
    private final S3BatchProcessor batchProcessor;

    @Inject
    public DataHubBatchJob(
            JobArguments arguments,
            SparkSessionProvider sparkSessionProvider,
            S3BatchProcessor batchProcessor
    ) {
        this.arguments = arguments;
        this.sparkSessionProvider = sparkSessionProvider;
        this.batchProcessor = batchProcessor;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubBatchJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        logger.info("Running DataHubBatchJob");

        try {
            val sparkSession = sparkSessionProvider.getConfiguredSparkSession(arguments.getLogLevel());
            val rawS3Path = arguments.getRawS3Path();

            val fileSystem = FileSystem.get(URI.create(rawS3Path), new Configuration());
            val fileIterator = fileSystem.listFiles(new Path(rawS3Path), true);

            while (fileIterator.hasNext()) {
                val filePath = fileIterator.next().getPath().toString();
                if (!filePath.contains("awsdms_")) {
                    logger.info("Processing file {}", filePath);

                    val startTime = System.currentTimeMillis();
                    val pathParts = filePath
                            .substring(rawS3Path.length(), filePath.lastIndexOf("/"))
                            .split("/");

                    val source = pathParts[0];
                    val table = pathParts[1];

                    val dataFrame = sparkSession.read().parquet(filePath).withColumn(OPERATION, lit(Load.getName()));
                    batchProcessor.processBatch(sparkSession, source, table, dataFrame);

                    logger.info("Processed file {} in {}ms", filePath, System.currentTimeMillis() - startTime);
                }
            }

            logger.info("DataHubBatchJob completed");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }

    }
}
