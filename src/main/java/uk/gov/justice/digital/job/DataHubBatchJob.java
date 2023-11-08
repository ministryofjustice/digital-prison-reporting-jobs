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
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
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
import static uk.gov.justice.digital.config.JobProperties.SPARK_JOB_NAME_PROPERTY;
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
        val rawS3Path = arguments.getRawS3Path();
        try {
            boolean runLocal = System.getProperty(SPARK_JOB_NAME_PROPERTY) == null;
            if(runLocal) {
                logger.info("Running locally");
                SparkConf sparkConf = new SparkConf().setAppName("DataHubBatchJob local").setMaster("local[*]");
                SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, arguments.getLogLevel());
                processByTable(rawS3Path, spark);
            } else {
                logger.info("Running in Glue");
                String jobName = properties.getSparkJobName();
                val glueContext = sparkSessionProvider.createGlueContext(jobName, arguments.getLogLevel());
                Job.init(jobName, glueContext, arguments.getConfig());
                SparkSession spark = glueContext.getSparkSession();
                processByTable(rawS3Path, spark);
                Job.commit();
            }
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
        logger.info("DataHubBatchJob completed in {}ms", System.currentTimeMillis() - startTime);
    }

    private void processByTable(String rawS3Path, SparkSession sparkSession) throws IOException {
        val startTime = System.currentTimeMillis();
        logger.info("Processing Raw {} by table", rawS3Path);
        Map<ImmutablePair<String, String>, List<String>> pathsByTable = discoverFilesToLoad(rawS3Path, sparkSession);

        for (val entry: pathsByTable.entrySet()) {
            val tableStartTime = System.currentTimeMillis();
            val schema = entry.getKey().getLeft();
            val table = entry.getKey().getRight();
            logger.info("Processing table {}.{}", schema, table);
            val filePaths = JavaConverters.asScalaIteratorConverter(entry.getValue().iterator()).asScala().toSeq();
            val dataFrame = sparkSession.read().parquet(filePaths);
            logger.info("Schema for {}.{}: \n{}", schema, table, dataFrame.schema().treeString());
            batchProcessor.processBatch(sparkSession, schema, table, dataFrame);
            logger.info("Processed table {}.{} in {}ms", schema, table, System.currentTimeMillis() - tableStartTime);
        }
        logger.info("Finished processing Raw {} by table in {}ms", rawS3Path, System.currentTimeMillis() - startTime);
    }

    @NotNull
    private static Map<ImmutablePair<String, String>, List<String>> discoverFilesToLoad(String rawS3Path, SparkSession sparkSession) throws IOException {
        val fileSystem = FileSystem.get(URI.create(rawS3Path), sparkSession.sparkContext().hadoopConfiguration());
        val fileIterator = fileSystem.listFiles(new Path(rawS3Path), true);
        Map<ImmutablePair<String, String>, List<String>> pathsByTable = new HashMap<>();
        val listPathsStartTime = System.currentTimeMillis();
        logger.info("Recursively enumerating load files");
        while (fileIterator.hasNext()) {
            Path path = fileIterator.next().getPath();
            val fileName = path.getName();
            val filePath = path.toUri().toString();
            if (fileName.startsWith("LOAD") && fileName.endsWith(".parquet")) {
                val pathParts = filePath
                        .substring(rawS3Path.length())
                        .split("/");
                val schema = pathParts[0];
                val table = pathParts[1];
                logger.info("Processing file {} for {}.{}", filePath, schema, table);
                val key = new ImmutablePair<>(schema, table);
                List<String> pathsSoFar;
                if (pathsByTable.containsKey(key)) {
                    pathsSoFar = pathsByTable.get(key);
                } else {
                    pathsSoFar = new ArrayList<>();
                    pathsByTable.put(key, pathsSoFar);
                }
                pathsSoFar.add(filePath);
            } else {
                logger.debug("Will skip file {}", filePath);
            }
        }
        logger.info("Finished recursively enumerating load files in {}ms", System.currentTimeMillis() - listPathsStartTime);
        return pathsByTable;
    }
}
