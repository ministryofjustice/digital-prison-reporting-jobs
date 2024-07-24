package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.util.Job;
import com.google.common.annotations.VisibleForTesting;
import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.SchemaNotFoundException;
import uk.gov.justice.digital.job.batchprocessing.ReloadDiffProcessor;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DmsOrchestrationService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Optional;

import static uk.gov.justice.digital.common.CommonDataFields.withCheckpointField;
import static uk.gov.justice.digital.common.CommonDataFields.withMetadataFields;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.config.JobProperties.SPARK_JOB_NAME_PROPERTY;

/**
 * Job that creates a diff between the raw and archived data.
 */
@CommandLine.Command(name = "CreateReloadDiffJob")
public class CreateReloadDiffJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CreateReloadDiffJob.class);
    private final JobArguments jobArguments;
    private final JobProperties properties;
    private final S3DataProvider dataProvider;
    private final SparkSessionProvider sparkSessionProvider;
    private final TableDiscoveryService tableDiscoveryService;
    private final DmsOrchestrationService dmsOrchestrationService;
    private final ReloadDiffProcessor reloadDiffProcessor;
    private final SourceReferenceService sourceReferenceService;

    @Inject
    public CreateReloadDiffJob(
            JobArguments jobArguments,
            JobProperties properties,
            S3DataProvider dataProvider,
            SparkSessionProvider sparkSessionProvider,
            TableDiscoveryService tableDiscoveryService,
            DmsOrchestrationService dmsOrchestrationService,
            ReloadDiffProcessor reloadDiffProcessor,
            SourceReferenceService sourceReferenceService
    ) {
        this.jobArguments = jobArguments;
        this.dataProvider = dataProvider;
        this.properties = properties;
        this.sparkSessionProvider = sparkSessionProvider;
        this.tableDiscoveryService = tableDiscoveryService;
        this.dmsOrchestrationService = dmsOrchestrationService;
        this.reloadDiffProcessor = reloadDiffProcessor;
        this.sourceReferenceService = sourceReferenceService;
    }

    public static void main(String[] args) {
        logger.info("Job starting");
        PicocliRunner.run(CreateReloadDiffJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        logger.info("Running CreateReloadDiffJob");
        try {
            boolean runLocal = System.getProperty(SPARK_JOB_NAME_PROPERTY) == null;
            if (runLocal) {
                logger.info("Running locally");
                SparkConf sparkConf = new SparkConf().setAppName("CreateReloadDiffJob local").setMaster("local[*]");
                SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, jobArguments);
                runJob(spark);
            } else {
                logger.info("Running in Glue");
                String jobName = properties.getSparkJobName();
                val glueContext = sparkSessionProvider.createGlueContext(jobName, jobArguments);
                Job.init(jobName, glueContext, jobArguments.getConfig());
                SparkSession spark = glueContext.getSparkSession();
                runJob(spark);
                Job.commit();
            }
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }

        logger.info("CreateReloadDiffJob completed");
    }

    @VisibleForTesting
    void runJob(SparkSession sparkSession) throws RuntimeException {
        val dmsStartTime = dmsOrchestrationService.getTaskStartTime(jobArguments.getDmsTaskId());
        val rawFilesPathsByTable = tableDiscoveryService.discoverBatchFilesToLoad(jobArguments.getRawS3Path(), sparkSession);
        val rawArchiveFilesPathsByTable = tableDiscoveryService.discoverBatchFilesToLoad(jobArguments.getRawArchiveS3Path(), sparkSession);

        for (val entry : rawFilesPathsByTable.entrySet()) {
            val schema = entry.getKey().getLeft();
            val table = entry.getKey().getRight();

            logger.info("Creating diffs for table {}.{}", schema, table);
            val archiveFilePaths = Optional.ofNullable(rawArchiveFilesPathsByTable.get(ImmutablePair.of(schema, table)))
                    .orElse(Collections.emptyList());

            val rawFilePaths = entry.getValue();
            if (!rawFilePaths.isEmpty()) {

                Optional<SourceReference> maybeSourceReference = sourceReferenceService.getSourceReference(schema, table);
                if (maybeSourceReference.isPresent()) {
                    SourceReference sourceReference = maybeSourceReference.get();
                    val diffOutputPath = createValidatedPath(jobArguments.getTempReloadS3Path(), jobArguments.getTempReloadOutputFolder());
                    val rawDataFrame = dataProvider.getBatchSourceData(sparkSession, rawFilePaths);
                    val rawArchiveDataFrame = archiveFilePaths.isEmpty() ?
                            sparkSession.emptyDataset(RowEncoder.apply(withCheckpointField(withMetadataFields(sourceReference.getSchema())))) :
                            dataProvider.getBatchSourceData(sparkSession, archiveFilePaths);

                    reloadDiffProcessor.createDiff(sourceReference, diffOutputPath, rawDataFrame, rawArchiveDataFrame, dmsStartTime);
                } else {
                    String errorMessage = String.format("Unable to retrieve schema for %s.%s", schema, table);
                    logger.warn(errorMessage);
                    throw new SchemaNotFoundException(errorMessage);
                }
            } else {
                logger.warn("No raw file paths found for table {}.{}", schema, table);
            }
        }
    }

}
