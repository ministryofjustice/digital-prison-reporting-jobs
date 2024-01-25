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
import java.time.Clock;
import java.util.*;

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
        logger.info("Job starting");
        PicocliRunner.run(S3DataDeletionJob.class, MicronautContext.withArgs(args).registerSingleton(Clock.class, Clock.systemUTC()));
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
        final ImmutableSet<String> allowedExtensions = jobArguments.getAllowedS3FileExtensions();

        Set<String> failedObjects = new HashSet<>();

        for (String bucketToDeleteFilesFrom : bucketsToDeleteFilesFrom) {
            List<String> listedFiles = s3FileService
                    .listFilesForConfig(bucketToDeleteFilesFrom, configuredTables, allowedExtensions, 0L);

            List<String> objectKeys = new ArrayList<>(listedFiles);

            logger.info("Deleting S3 objects from {} ", bucketToDeleteFilesFrom);
            failedObjects = s3FileService.deleteObjects(objectKeys, bucketToDeleteFilesFrom);
        }

        if (failedObjects.isEmpty()) {
            logger.info("Successfully deleted S3 files");
        } else {
            logger.warn("Not all S3 files were deleted");
            System.exit(1);
        }
    }
}
