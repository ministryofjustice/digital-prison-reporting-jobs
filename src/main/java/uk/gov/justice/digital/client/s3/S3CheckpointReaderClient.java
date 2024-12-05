package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import jakarta.inject.Inject;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.common.CheckpointFile;

import javax.inject.Singleton;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.Optional;
import java.util.Collections;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Singleton
public class S3CheckpointReaderClient {

    private static final Logger logger = LoggerFactory.getLogger(S3CheckpointReaderClient.class);

    private final AmazonS3 s3;

    // Extracts the ordinal and optional compact extension from the checkpoint file name. Example checkpoint file names: 1 or 1.compact
    private final Pattern checkpointFileRegexPattern = Pattern.compile("^.*\\/(\\d+)(.compact)?$");

    // Extracts the committed file path from a line in the checkpoint file
    private final Pattern committedFileRegexPattern = Pattern.compile("^\\{\"path\":\"s3:\\/\\/([a-zA-Z-]+)\\/(.*)\",.*");

    @Inject
    public S3CheckpointReaderClient(S3ClientProvider clientProvider) {
        this.s3 = clientProvider.getClient();
    }

    public Set<String> getCommittedFiles(String checkpointBucket, List<String> checkpointFiles) {
        List<CheckpointFile> reverseOrderedCheckpointFiles = orderCheckpointFilesInReverseOrdering(checkpointFiles);

        Set<String> committedFiles = new HashSet<>();
        for (CheckpointFile checkpointFile : reverseOrderedCheckpointFiles) {
            String committedFileContent = s3.getObjectAsString(checkpointBucket, checkpointFile.getS3key());
            List<String> linesContainingCommittedFiles = getLinesContainingCommittedFiles(committedFileContent);

            committedFiles.addAll(getCommittedFiles(linesContainingCommittedFiles));

            // Spark streaming periodically creates a 'compact' file which provides a summary of committed files contained in previous commit files.
            // The traverse of the checkpoints is done up to and including the first encountered 'compact' file.
            if (checkpointFile.isCompact()) {
                return committedFiles;
            }
        }

        return committedFiles;
    }

    @NotNull
    private List<CheckpointFile> orderCheckpointFilesInReverseOrdering(List<String> checkpointFiles) {
        return checkpointFiles.stream()
                .filter(checkpointFile -> !checkpointFile.toLowerCase().endsWith(".tmp"))
                .map(checkpointFile -> {
                    Matcher matcher = checkpointFileRegexPattern.matcher(checkpointFile);
                    if (matcher.matches()) {
                        boolean isCompactFile = Optional.ofNullable(matcher.group(2)).isPresent();
                        return new CheckpointFile(Long.parseLong(matcher.group(1)), isCompactFile, checkpointFile);
                    } else {
                        throw new IllegalStateException("Failed to extract file name from " + checkpointFile);
                    }
                }).sorted(Collections.reverseOrder())
                .collect(Collectors.toList());
    }

    private Set<String> getCommittedFiles(List<String> linesContainingCommittedFiles) {
        Set<String> committedFiles = new HashSet<>();
        for (String line : linesContainingCommittedFiles) {
            Matcher matcher = committedFileRegexPattern.matcher(line);
            if (matcher.matches()) {
                committedFiles.add(matcher.group(2));
            } else {
                logger.warn("Unable to extract committed file from {}", line);
            }
        }

        return committedFiles;
    }

    @NotNull
    private static List<String> getLinesContainingCommittedFiles(String committedFileContent) {
        return Arrays.stream(committedFileContent.split("\n"))
                .filter(line -> line.contains("path"))
                .collect(Collectors.toList());
    }
}
