package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import io.micronaut.configuration.picocli.PicocliRunner;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Job that moves parquet files from a source bucket to a destination bucket.
 */
@CommandLine.Command(name = "S3FileTransferJob")
public class S3FileTransferJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(S3FileTransferJob.class);
    private final ConfigService configService;
    private final S3FileService s3FileService;
    private final JobArguments jobArguments;

    @Inject
    public S3FileTransferJob(
            ConfigService configService,
            S3FileService s3FileService,
            JobArguments jobArguments
    ) {
        this.configService = configService;
        this.s3FileService = s3FileService;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        logger.info("Job starting");
        PicocliRunner.run(S3FileTransferJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        try {
            logger.info("S3FileTransferJob running");

            transferFiles();

            logger.info("S3FileTransferJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }

    private void transferFiles() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService
                .getConfiguredTables(jobArguments.getConfigKey());

        final String sourceBucket = jobArguments.getTransferSourceBucket();
        final String destinationBucket = jobArguments.getTransferDestinationBucket();
        final Long retentionDays = jobArguments.getFileTransferRetentionDays();

        List<String> objectKeys = new ArrayList<>();
        if (configuredTables.isEmpty()) {
            // When no config is provided, all files in s3 bucket are archived
            logger.info("Listing files in S3 source location: {}", sourceBucket);
            objectKeys.addAll(s3FileService.listParquetFiles(sourceBucket, retentionDays));
        } else {
            // When config is provided, only files belonging to the configured tables are archived
            objectKeys.addAll(s3FileService.listParquetFilesForConfig(sourceBucket, configuredTables, retentionDays));
        }

        logger.info("Moving S3 objects older than {} day(s) from {} to {}", retentionDays, sourceBucket, destinationBucket);
        Set<String> failedObjects = s3FileService.moveObjects(objectKeys, sourceBucket, destinationBucket);

        if (failedObjects.isEmpty()) {
            logger.info("Successfully moved {} S3 files", objectKeys.size());
        } else {
            logger.warn("Not all S3 files were moved");
            System.exit(1);
        }
    }
}
