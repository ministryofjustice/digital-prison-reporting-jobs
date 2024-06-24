package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import io.micronaut.configuration.picocli.PicocliRunner;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.service.CheckpointReaderService;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Job that checks if all raw files have been processed.
 */
@CommandLine.Command(name = "UnprocessedRawFilesCheckJob")
public class UnprocessedRawFilesCheckJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(UnprocessedRawFilesCheckJob.class);
    private final ConfigService configService;
    private final S3FileService s3FileService;
    private final CheckpointReaderService checkpointReaderService;
    private final JobArguments jobArguments;

    @Inject
    public UnprocessedRawFilesCheckJob(
            ConfigService configService,
            S3FileService s3FileService,
            CheckpointReaderService checkpointReaderService,
            JobArguments jobArguments
    ) {
        this.configService = configService;
        this.s3FileService = s3FileService;
        this.checkpointReaderService = checkpointReaderService;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        logger.info("Job starting");
        PicocliRunner.run(UnprocessedRawFilesCheckJob.class, MicronautContext.withArgs(args).registerSingleton(Clock.class, Clock.systemUTC()));
    }

    @Override
    public void run() {
        try {
            logger.info("UnprocessedRawFilesCheckJob running");
            int maxAttempts = jobArguments.orchestrationMaxAttempts();
            int waitIntervalSeconds = jobArguments.orchestrationWaitIntervalSeconds();

            if (!waitForRawFilesToBeProcessed(maxAttempts, waitIntervalSeconds)) {
                String errorMessage = String.format("Failed to verify all files are processed after %d attempts", maxAttempts);
                logger.error(errorMessage);
                System.exit(1);
            }

            logger.info("UnprocessedRawFilesCheckJob finished");
        } catch (InterruptedException e) {
            logger.error("Interrupted exception during job run", e);
            Thread.currentThread().interrupt();
            System.exit(1);
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }

    private boolean waitForRawFilesToBeProcessed(int maxAttempts, int waitIntervalSeconds) throws InterruptedException {
        for (int attempts = 0; attempts < maxAttempts; attempts++) {
            if (verifyRawFilesProcessed()) {
                logger.info("All raw files have been processed");
                return true;
            }

            logger.info("Waiting for all raw files to be processed");
            TimeUnit.SECONDS.sleep(waitIntervalSeconds);
        }

        return false;
    }

    private boolean verifyRawFilesProcessed() {
        String rawBucket = jobArguments.getTransferSourceBucket();
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService
                .getConfiguredTables(jobArguments.getConfigKey());

        Set<String> committedFiles = new HashSet<>();

        for (ImmutablePair<String, String> configuredTable : configuredTables) {
            String sourceTable = configuredTable.left + "." + configuredTable.right;
            committedFiles.addAll(checkpointReaderService.getCommittedFilesForTable(configuredTable));
            logger.info("Found {} committed files for {}", committedFiles.size(), sourceTable);
        }

        logger.info("Listing files in raw bucket");
        List<String> rawFiles = s3FileService
                .listFilesForConfig(rawBucket, "", configuredTables, ImmutableSet.of(".parquet"), Duration.ZERO);

        return committedFiles.containsAll(rawFiles);
    }
}