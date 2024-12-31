package uk.gov.justice.digital.service;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3CheckpointReaderClient;
import uk.gov.justice.digital.client.s3.S3ObjectClient;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

import static uk.gov.justice.digital.common.RegexPatterns.checkpointRegexPattern;
import static uk.gov.justice.digital.common.RegexPatterns.matchAllFiles;
import static uk.gov.justice.digital.common.StreamingQuery.getQueryCheckpointPath;

@Singleton
public class CheckpointReaderService {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointReaderService.class);

    private final S3ObjectClient s3ObjectClient;
    private final S3CheckpointReaderClient checkpointReaderClient;
    private final JobArguments jobArguments;
    private final Clock clock;

    @Inject
    public CheckpointReaderService(
            S3ObjectClient s3ObjectClient,
            S3CheckpointReaderClient checkpointReaderClient,
            JobArguments jobArguments,
            Clock clock
    ) {
        this.s3ObjectClient = s3ObjectClient;
        this.checkpointReaderClient = checkpointReaderClient;
        this.jobArguments = jobArguments;
        this.clock = clock;
    }

    public Set<String> getCommittedFilesForTable(ImmutablePair<String, String> configuredTable) {
        String checkpointLocation = jobArguments.getCheckpointLocation();
        Matcher matcher = checkpointRegexPattern.matcher(checkpointLocation);

        if (matcher.matches()) {
            String checkpointBucket = matcher.group(1);
            String checkpointPath = getQueryCheckpointPath(matcher.group(2), configuredTable.left, configuredTable.right) + "/sources/0/";

            logger.info("Reading committed files from {}", checkpointPath);
            List<String> checkpointFiles = s3ObjectClient.getObjectsOlderThan(
                    checkpointBucket,
                    checkpointPath,
                    matchAllFiles,
                    Duration.ZERO,
                    clock
            );
            logger.info("Found {} checkpoint files in {}", checkpointFiles.size(), checkpointPath);

            return checkpointReaderClient.getCommittedFiles(checkpointBucket, checkpointFiles);
        } else {
            throw new IllegalArgumentException("Could not resolve checkpoint bucket and folder from " + checkpointLocation);
        }
    }

}
