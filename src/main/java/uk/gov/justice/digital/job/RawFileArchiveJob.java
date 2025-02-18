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
import java.util.List;
import java.util.Set;
import java.util.HashSet;
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
        String archivedFilesBucket = jobArguments.getJobsS3Bucket();
        Duration retentionPeriod = jobArguments.getRawFileRetentionPeriod();
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService
                .getConfiguredTables(jobArguments.getConfigKey());

        Set<FileLastModifiedDate> rawFiles = new HashSet<>(s3FileService
                .listFilesBeforePeriod(rawBucket, "", configuredTables, parquetFileRegex, Duration.ZERO));

        Set<String> previouslyArchivedFiles = s3FileService
                .getPreviousArchivedKeys(archivedFilesBucket, jobArguments.getConfigKey());

        // Exclude the files which were already archived within the past specified period
        List<String> newFilesToArchive = rawFiles.stream()
                .filter(elem -> !previouslyArchivedFiles.contains(elem.key))
                .map(x -> x.key)
                .collect(Collectors.toList());

        logger.info("Archiving {} files in S3 source location: {}", newFilesToArchive.size(), rawBucket);
        Set<String> failedFiles = s3FileService.copyObjects(newFilesToArchive, rawBucket, "", archiveBucket, "", false);
        logger.info("Successfully archived {} S3 files", newFilesToArchive.size());

        if (failedFiles.isEmpty()) {
            List<String> committedFiles = getCommittedFilesForConfig(configuredTables);

            // Only delete old files which have been committed to the checkpoint
            List<String> filesToDelete = rawFiles.stream()
                    .filter(x -> x.lastModifiedDateTime.isBefore(LocalDateTime.now(clock).minus(retentionPeriod)))
                    .filter(x -> committedFiles.contains(x.key))
                    .map(x -> x.key)
                    .collect(Collectors.toList());

            logger.info("Deleting {} files older than {} in S3 source location: {}", filesToDelete.size(), retentionPeriod, rawBucket);
            Set<String> failedDeletes = s3FileService.deleteObjects(filesToDelete, rawBucket);
            logger.info("Successfully deleted {} S3 files", filesToDelete.size());

            // The updated archived keys to save includes the:
            // previously archived files + new files to archive + files which were deleted
            // The files which failed to be deleted are left in to avoid re-archiving them in the next run
            Set<String> archivedKeysToSave = new HashSet<>(previouslyArchivedFiles);
            archivedKeysToSave.addAll(newFilesToArchive);
            filesToDelete.removeAll(failedDeletes);
            filesToDelete.forEach(archivedKeysToSave::remove);

            s3FileService.saveArchivedKeys(archivedFilesBucket, jobArguments.getConfigKey(), new ArrayList<>(archivedKeysToSave));
            logger.info("Saved archived keys");
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
}
