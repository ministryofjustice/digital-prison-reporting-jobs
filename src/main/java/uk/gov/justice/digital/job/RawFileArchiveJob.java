package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;
import uk.gov.justice.digital.service.CheckpointReaderService;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;

/**
 * Job that archives raw s3 files to a destination bucket.
 */
@CommandLine.Command(name = "RawFileArchiveJob")
public class RawFileArchiveJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RawFileArchiveJob.class);
    private final ConfigService configService;
    private final S3FileService s3FileService;
    private final CheckpointReaderService checkpointReaderService;
    private final Clock clock;
    private final JobArguments jobArguments;

    @Inject
    public RawFileArchiveJob(
            ConfigService configService,
            S3FileService s3FileService,
            CheckpointReaderService checkpointReaderService,
            Clock clock,
            JobArguments jobArguments
    ) {
        this.configService = configService;
        this.s3FileService = s3FileService;
        this.checkpointReaderService = checkpointReaderService;
        this.clock = clock;
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
        String archiveBucket = jobArguments.getTransferDestinationBucket();
        Duration retentionPeriod = jobArguments.getRawFileRetentionPeriod();
        Duration archivedFilesCheckDuration = jobArguments.getArchivedFilesCheckDuration();
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService
                .getConfiguredTables(jobArguments.getConfigKey());

        Set<FileLastModifiedDate> rawFiles = new HashSet<>(s3FileService
                .listFilesBeforePeriod(rawBucket, "", configuredTables, parquetFileRegex, Duration.ZERO));

        Set<String> recentlyArchivedFiles = new HashSet<>(s3FileService
                .listFilesAfterPeriod(archiveBucket, "", configuredTables, parquetFileRegex, archivedFilesCheckDuration))
                .stream()
                .map(x -> x.key)
                .collect(Collectors.toSet());

        // Exclude the files which were already archived within the past specified period
        List<String> filesToArchive = rawFiles.stream()
                .filter(elem -> !recentlyArchivedFiles.contains(elem.key))
                .map(x -> x.key)
                .collect(Collectors.toList());

        logger.info("Archiving {} files in S3 source location: {}", filesToArchive.size(), rawBucket);
        Set<String> failedFiles = s3FileService.copyObjects(filesToArchive, rawBucket, "", archiveBucket, "", false);

        if (failedFiles.isEmpty()) {
            logger.info("Successfully archived {} S3 files", filesToArchive.size());
        } else {
            logger.warn("Not all files were archived");
            failedFiles.forEach(logger::warn);
            System.exit(1);
        }

        List<String> committedFiles = getCommittedFilesForConfig(configuredTables);

        // Only delete old files which have been committed to the checkpoint
        List<String> filesToDelete = rawFiles.stream()
                .filter(x -> x.lastModifiedDateTime.isBefore(LocalDateTime.now(clock).minus(retentionPeriod)))
                .filter(x -> committedFiles.contains(x.key))
                .map(x -> x.key)
                .collect(Collectors.toList());

        logger.info("Deleting {} files older than {} in S3 source location: {}", filesToDelete.size(), retentionPeriod, rawBucket);
        s3FileService.deleteObjects(filesToDelete, rawBucket);
        logger.info("Successfully deleted {} S3 files", filesToDelete.size());
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
}
