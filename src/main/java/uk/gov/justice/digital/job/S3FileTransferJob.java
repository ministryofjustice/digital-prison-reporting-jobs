package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import javax.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Job that moves/copies s3 files from a source bucket to a destination bucket.
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
        PicocliMicronautExecutor.execute(S3FileTransferJob.class, args);
    }

    @Override
    public void run() {
        try {
            logger.info("S3FileTransferJob running");

            copyFiles();

            logger.info("S3FileTransferJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }

    private void copyFiles() {
        Optional<String> optionalConfigKey = jobArguments.getOptionalConfigKey();
        final String sourceBucket = jobArguments.getTransferSourceBucket();
        final String sourcePrefix = jobArguments.getSourcePrefix();
        final String destinationBucket = jobArguments.getTransferDestinationBucket();
        final String destinationPrefix = jobArguments.getTransferDestinationPrefix();
        final Duration retentionPeriod = jobArguments.getFileTransferRetentionPeriod();
        final boolean deleteCopiedFiles = jobArguments.getFileTransferDeleteCopiedFilesFlag();
        final ImmutableSet<String> allowedExtensions = jobArguments.getAllowedS3FileExtensions();

        List<String> objectKeys = new ArrayList<>();
        if (optionalConfigKey.isPresent()) {
            // When config is provided, only files belonging to the configured tables are archived
            ImmutableSet<ImmutablePair<String, String>> configuredTables = configService
                    .getConfiguredTables(optionalConfigKey.get());
            objectKeys.addAll(s3FileService.listFilesForConfig(sourceBucket, sourcePrefix, configuredTables, allowedExtensions, retentionPeriod));
        } else {
            // When no config is provided, all files in s3 bucket are archived
            logger.info("Listing files in S3 source location: {}", sourceBucket);
            objectKeys.addAll(s3FileService.listFiles(sourceBucket, sourcePrefix, allowedExtensions, retentionPeriod));
        }

        logger.info("Processing S3 objects older than {} from {} to {}", retentionPeriod, sourceBucket, destinationBucket);
        Set<String> failedObjects = s3FileService
                .copyObjects(objectKeys, sourceBucket, sourcePrefix, destinationBucket, destinationPrefix, deleteCopiedFiles);

        if (failedObjects.isEmpty()) {
            logger.info("Successfully processed {} S3 files", objectKeys.size());
        } else {
            logger.warn("Not all S3 files were processed");
            System.exit(1);
        }
    }
}
