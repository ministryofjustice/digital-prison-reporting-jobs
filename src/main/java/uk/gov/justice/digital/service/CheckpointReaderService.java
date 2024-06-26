package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableSet;
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
import java.util.regex.Pattern;

import static uk.gov.justice.digital.common.StreamingQuery.getQueryCheckpointPath;

@Singleton
public class CheckpointReaderService {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointReaderService.class);

    private final S3ObjectClient s3ObjectClient;
    private final S3CheckpointReaderClient checkpointReaderClient;
    private final JobArguments jobArguments;
    private final Clock clock;
    // Extracts the bucket and the folder path from the checkpoint location
    // As an example s3://dpr-glue-jobs-development/checkpoint/dpr-reporting-hub-cdc-establishments-development/
    // is extracted to:
    // bucket - dpr-glue-jobs-development
    // folder path - checkpoint/dpr-reporting-hub-cdc-establishments-development/
    private final Pattern checkpointRegexPattern = Pattern.compile("^s3[A-Za-z]?:\\/\\/([A-Za-z-]+)\\/([\\S\\s]+\\S+)");

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
                    ImmutableSet.of("*"),
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
