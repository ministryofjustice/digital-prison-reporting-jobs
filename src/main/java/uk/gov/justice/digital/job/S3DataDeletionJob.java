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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Job that deletes s3 files from a list of bucket(s).
 */
@CommandLine.Command(name = "S3DataDeletionJob")
public class S3DataDeletionJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(S3DataDeletionJob.class);
    private final ConfigService configService;
    private final S3FileService s3FileService;
    private final JobArguments jobArguments;

    @Inject
    public S3DataDeletionJob(
            ConfigService configService,
            S3FileService s3FileService,
            JobArguments jobArguments
    ) {
        this.configService = configService;
        this.s3FileService = s3FileService;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(S3DataDeletionJob.class, args);
    }

    @Override
    public void run() {
        try {
            logger.info("S3DataDeletionJob running");

            deleteFiles();

            logger.info("S3DataDeletionJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }

    private void deleteFiles() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService
                .getConfiguredTables(jobArguments.getConfigKey());

        final ImmutableSet<String> bucketsToDeleteFilesFrom = jobArguments.getBucketsToDeleteFilesFrom();
        final String sourcePrefix = jobArguments.getSourcePrefix();
        final Pattern allowedFileNameRegex = jobArguments.getAllowedS3FileNameRegex();

        Set<String> failedObjects = new HashSet<>();

        for (String bucketToDeleteFilesFrom : bucketsToDeleteFilesFrom) {
            List<String> listedFiles = s3FileService
                    .listFilesBeforePeriod(bucketToDeleteFilesFrom, sourcePrefix, configuredTables, allowedFileNameRegex, Duration.ZERO)
                    .stream()
                    .map(x -> x.key)
                    .collect(Collectors.toList());

            logger.info("Deleting S3 objects from {}", bucketToDeleteFilesFrom);
            failedObjects = s3FileService.deleteObjects(listedFiles, bucketToDeleteFilesFrom);
        }

        if (failedObjects.isEmpty()) {
            logger.info("Successfully deleted S3 files");
        } else {
            logger.warn("Not all S3 files were deleted");
            System.exit(1);
        }
    }
}
