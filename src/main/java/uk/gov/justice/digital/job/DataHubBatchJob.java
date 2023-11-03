package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.util.Job;
import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.job.batchprocessing.S3BatchProcessor;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.Load;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.*;
import scala.collection.JavaConverters;

@Singleton
@CommandLine.Command(name = "DataHubBatchJob")
public class DataHubBatchJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubBatchJob.class);

    private final JobArguments arguments;

    private final JobProperties properties;
    private final SparkSessionProvider sparkSessionProvider;
    private final S3BatchProcessor batchProcessor;

    @Inject
    public DataHubBatchJob(
            JobArguments arguments,
            JobProperties properties,
            SparkSessionProvider sparkSessionProvider,
            S3BatchProcessor batchProcessor
    ) {
        this.arguments = arguments;
        this.properties = properties;
        this.sparkSessionProvider = sparkSessionProvider;
        this.batchProcessor = batchProcessor;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubBatchJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        val startTime = System.currentTimeMillis();
        logger.info("Running DataHubBatchJob");
        try {
            String jobName = properties.getSparkJobName();
            val glueContext = sparkSessionProvider.createGlueContext(jobName, arguments.getLogLevel());
            Job.init(jobName, glueContext, arguments.getConfig());
            val sparkSession = glueContext.getSparkSession();
            val rawS3Path = arguments.getRawS3Path();

            val fileSystem = FileSystem.get(URI.create(rawS3Path), sparkSession.sparkContext().hadoopConfiguration());
            val fileIterator = fileSystem.listFiles(new Path(rawS3Path), true);

            if(arguments.isBatchProcessByTable()) {
                processByTable(fileIterator, rawS3Path, sparkSession);
            } else {
                processFileAtATime(fileIterator, rawS3Path, sparkSession);
            }
            Job.commit();
            logger.info("DataHubBatchJob completed in {}ms", System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }

    }

    private void processByTable(RemoteIterator<LocatedFileStatus> fileIterator, String rawS3Path, SparkSession sparkSession) throws IOException {
        logger.info("Processing Raw {} by table", rawS3Path);
        Map<ImmutablePair<String, String>, List<String>> pathsByTable = new HashMap<>();
        val listPathsStartTime = System.currentTimeMillis();
        logger.info("Recursively enumerating load files");
        while (fileIterator.hasNext()) {
            val filePath = fileIterator.next().getPath().toUri().toString();
            if (filePath.endsWith(".parquet")) {
                val pathParts = filePath
                        .substring(rawS3Path.length(), filePath.lastIndexOf("/"))
                        .split("/");
                val source = pathParts[0];
                val table = pathParts[1];
                val fileName = pathParts[2];
                if (fileName.startsWith("LOAD")) {
                    logger.info("Will process file {}", filePath);
                    val key = new ImmutablePair<>(source, table);
                    List<String> pathsSoFar;
                    if (pathsByTable.containsKey(key)) {
                        pathsSoFar = pathsByTable.get(key);
                    } else {
                        pathsSoFar = new ArrayList<>();
                        pathsByTable.put(key, pathsSoFar);
                    }
                    pathsSoFar.add(filePath);
                }
            }
        }
        logger.info("Finished recursively enumerating load files in {}ms", System.currentTimeMillis() - listPathsStartTime);

        for (val entry: pathsByTable.entrySet()) {
            val startTime = System.currentTimeMillis();
            val source = entry.getKey().getLeft();
            val table = entry.getKey().getRight();
            logger.info("Processing table {}.{}", source, table);
            val filePaths = JavaConverters.asScalaIteratorConverter(entry.getValue().iterator()).asScala().toSeq();
            val dataFrame = sparkSession.read().parquet(filePaths).withColumn(OPERATION, lit(Load.getName()));
            batchProcessor.processBatch(sparkSession, source, table, dataFrame);
            logger.info("Processed table {}.{} in {}ms", source, table, System.currentTimeMillis() - startTime);
        }
        logger.info("Finished processing Raw {} by table", rawS3Path);
    }

    private void processFileAtATime(RemoteIterator<LocatedFileStatus> fileIterator, String rawS3Path, SparkSession sparkSession) throws IOException {
        logger.info("Processing Raw {} a file at a time", rawS3Path);
        while (fileIterator.hasNext()) {
            val filePath = fileIterator.next().getPath().toString();
            if (filePath.endsWith("LOAD*.parquet")) {
                val startTime = System.currentTimeMillis();
                logger.info("Processing file {}", filePath);

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
        logger.info("Finished processing Raw {} a file at a time", rawS3Path);
    }
}
