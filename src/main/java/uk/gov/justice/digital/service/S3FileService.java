package uk.gov.justice.digital.service;

import com.amazonaws.AmazonServiceException;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3FileTransferClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Clock;
import java.util.*;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.client.s3.S3FileTransferClient.DELIMITER;

@Singleton
public class S3FileService {

    private static final Logger logger = LoggerFactory.getLogger(S3FileService.class);
    private final S3FileTransferClient s3Client;
    private final Clock clock;

    public static final String FILE_EXTENSION = ".parquet";

    @Inject
    public S3FileService(
            S3FileTransferClient s3Client,
            Clock clock
    ) {
        this.s3Client = s3Client;
        this.clock = clock;
    }

    public List<String> listParquetFiles(String bucket, Long retentionDays) {
        return s3Client.getObjectsOlderThan(bucket, FILE_EXTENSION, retentionDays, clock);
    }

    public List<String> listParquetFilesForConfig(
            String sourceBucket,
            ImmutableSet<ImmutablePair<String, String>> configuredTables,
            Long retentionDays
    ) {
        return configuredTables.stream()
                .flatMap(configuredTable -> listFilesForTable(sourceBucket, retentionDays, configuredTable).stream())
                .collect(Collectors.toList());
    }

    public Set<String> copyObjects(
            List<String> objectKeys,
            String sourceBucket,
            String destinationBucket,
            boolean deleteCopiedFiles
    ) {
        Set<String> failedObjects = new HashSet<>();

        for (String objectKey : objectKeys) {
            try {
                s3Client.copyObject(objectKey, sourceBucket, destinationBucket);
                if (deleteCopiedFiles) s3Client.deleteObject(objectKey, sourceBucket);
            } catch (AmazonServiceException e) {
                logger.warn("Failed to move S3 object {}: {}", objectKey, e.getErrorMessage());
                failedObjects.add(objectKey);
            }
        }

        return failedObjects;
    }

    public Set<String> deleteObjects(List<String> objectKeys, String sourceBucket) {
        Set<String> failedObjects = new HashSet<>();

        for (String objectKey : objectKeys) {
            try {
                s3Client.deleteObject(objectKey, sourceBucket);
            } catch (AmazonServiceException e) {
                logger.warn("Failed to delete S3 object {}: {}", objectKey, e.getErrorMessage());
                failedObjects.add(objectKey);
            }
        }

        return failedObjects;
    }

    private List<String> listFilesForTable(
            String sourceBucket,
            Long retentionDays,
            ImmutablePair<String, String> configuredTable
    ) {
        String tableKey = configuredTable.left + DELIMITER + configuredTable.right + DELIMITER;
        logger.info("Listing files in S3 source location {} for table {}", sourceBucket, tableKey);
        return s3Client.getObjectsOlderThan(
                sourceBucket,
                tableKey,
                FILE_EXTENSION,
                retentionDays,
                clock
        );
    }
}
