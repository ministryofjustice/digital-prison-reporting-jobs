package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.RawArchiveLocationService;
import uk.gov.justice.digital.service.S3FileService;

import javax.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;

/*
 * Archives and removes every raw file for a domain right now, no retention/checkpoint checks - used to clear
 * the raw zone before a reingestion. Unlike RawFileArchiveJob it doesn't need an archived-keys manifest since
 * it just re-lists the raw zone on each run.
 */
@CommandLine.Command(name = "RawZoneReingestionFlushJob")
public class RawZoneReingestionFlushJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RawZoneReingestionFlushJob.class);

    private final ConfigService configService;
    private final S3FileService s3FileService;
    private final JobArguments jobArguments;
    private final RawArchiveLocationService rawArchiveLocationService;

    @Inject
    public RawZoneReingestionFlushJob(
            ConfigService configService,
            S3FileService s3FileService,
            JobArguments jobArguments,
            RawArchiveLocationService rawArchiveLocationService
    ) {
        this.configService = configService;
        this.s3FileService = s3FileService;
        this.jobArguments = jobArguments;
        this.rawArchiveLocationService = rawArchiveLocationService;
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(RawZoneReingestionFlushJob.class, args);
    }

    @Override
    public void run() {
        try {
            logger.info("RawZoneReingestionFlushJob running");
            flushRawZone();
            logger.info("RawZoneReingestionFlushJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }

    private void flushRawZone() {
        String rawBucket = jobArguments.getTransferSourceBucket();
        String archiveBucket = jobArguments.getTransferDestinationBucket();
        String configKey = jobArguments.getConfigKey();

        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(configKey);

        List<String> rawFiles = s3FileService
                .listFilesBeforePeriod(rawBucket, "", configuredTables, parquetFileRegex, Duration.ZERO)
                .stream()
                .map(file -> file.key)
                .toList();

        logger.info("Flushing {} raw files for domain {} from {} to {}", rawFiles.size(), configKey, rawBucket, archiveBucket);
        Set<String> failedFiles = s3FileService.copyObjects(
                rawFiles, rawBucket, archiveBucket, true, rawArchiveLocationService::applyVersionToKey
        );

        if (failedFiles.isEmpty()) {
            logger.info("Successfully archived and removed {} raw files for domain {}", rawFiles.size(), configKey);
        } else {
            logger.warn("Not all raw files were archived and removed for domain {}", configKey);
            failedFiles.forEach(logger::warn);
            System.exit(1);
        }
    }
}
