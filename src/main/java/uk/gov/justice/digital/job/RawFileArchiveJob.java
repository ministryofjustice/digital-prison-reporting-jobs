package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.CheckpointReaderService;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import javax.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Job that archives raw s3 files to a destination bucket.
 */
@CommandLine.Command(name = "RawFileArchiveJob")
public class RawFileArchiveJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RawFileArchiveJob.class);
    private final ConfigService configService;
    private final S3FileService s3FileService;
    private final CheckpointReaderService checkpointReaderService;
    private final JobArguments jobArguments;

    @Inject
    public RawFileArchiveJob(
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
        PicocliMicronautExecutor.execute(RawFileArchiveJob.class, args);
    }

    @Override
    public void run() {
        try {
            logger.info("RawFileArchiveJob running");
            archiveFiles();
            logger.info("RawFileArchiveJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }

    private void archiveFiles() {
        String rawBucket = jobArguments.getTransferSourceBucket();
        String destinationBucket = jobArguments.getTransferDestinationBucket();
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService
                .getConfiguredTables(jobArguments.getConfigKey());

        List<String> committedFiles = getCommittedFilesForConfig(configuredTables);
        Set<String> rawFiles = new HashSet<>(s3FileService
                .listFilesForConfig(rawBucket, "", configuredTables, ImmutableSet.of(".parquet"), Duration.ZERO));

        List<String> filesToArchive = getCommittedFilesNotAlreadyArchived(committedFiles, rawFiles);

        logger.info("Archiving files in S3 source location: {}", rawBucket);
        Set<String> failedFiles = s3FileService.copyObjects(filesToArchive, rawBucket, "", destinationBucket, "", true);

        if (failedFiles.isEmpty()) {
            logger.info("Successfully archived {} S3 files", committedFiles.size());
        } else {
            logger.warn("Not all files were archived");
            failedFiles.forEach(logger::warn);
            System.exit(1);
        }
    }

    @NotNull
    private List<String> getCommittedFilesForConfig(ImmutableSet<ImmutablePair<String, String>> configuredTables) {
        List<String> committedFiles = new ArrayList<>();
        for (ImmutablePair<String, String> configuredTable : configuredTables) {
            logger.info("Getting committed files for {}.{}", configuredTable.left, configuredTable.right);
            committedFiles.addAll(new ArrayList<>(checkpointReaderService.getCommittedFilesForTable(configuredTable)));
        }
        return committedFiles;
    }

    @NotNull
    private List<String> getCommittedFilesNotAlreadyArchived(List<String> committedFiles, Set<String> rawFiles) {
        List<String> filesToArchive = new ArrayList<>();
        for (String committedFile : committedFiles) {
            if (rawFiles.contains(committedFile)) {
                filesToArchive.add(committedFile);
            }
        }
        return filesToArchive;
    }
}
