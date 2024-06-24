package uk.gov.justice.digital.service;

import com.amazonaws.AmazonServiceException;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3ObjectClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.client.s3.S3ObjectClient.DELIMITER;

@Singleton
public class S3FileService {

    private static final Logger logger = LoggerFactory.getLogger(S3FileService.class);
    private final S3ObjectClient s3Client;
    private final Clock clock;

    @Inject
    public S3FileService(
            S3ObjectClient s3Client,
            Clock clock
    ) {
        this.s3Client = s3Client;
        this.clock = clock;
    }

    public List<String> listFiles(String bucket, String sourcePrefix, ImmutableSet<String> allowedExtensions, Duration retentionPeriod) {
        return s3Client.getObjectsOlderThan(bucket, sourcePrefix, allowedExtensions, retentionPeriod, clock);
    }

    public List<String> listFilesForConfig(
            String sourceBucket,
            String sourcePrefix,
            ImmutableSet<ImmutablePair<String, String>> configuredTables,
            ImmutableSet<String> allowedExtensions,
            Duration retentionPeriod
    ) {
        return configuredTables.stream()
                .flatMap(configuredTable -> listFilesForTable(sourceBucket, sourcePrefix, allowedExtensions, retentionPeriod, configuredTable).stream())
                .collect(Collectors.toList());
    }

    public Set<String> copyObjects(
            List<String> objectKeys,
            String sourceBucket,
            String sourcePrefix,
            String destinationBucket,
            String destinationPrefix,
            boolean deleteCopiedFiles
    ) {
        Set<String> failedObjects = new HashSet<>();

        for (String objectKey : objectKeys) {
            String destinationKey;
            try {
                if (!sourcePrefix.isEmpty()) {
                    destinationKey = destinationPrefix.isEmpty() ?
                            objectKey.replaceFirst(sourcePrefix + DELIMITER, destinationPrefix) :
                            objectKey.replaceFirst(sourcePrefix, destinationPrefix);
                } else {
                    destinationKey = destinationPrefix.isEmpty() ?
                            objectKey.replaceFirst(sourcePrefix, destinationPrefix) :
                            destinationPrefix + DELIMITER + objectKey;
                }

                s3Client.copyObject(objectKey, destinationKey, sourceBucket, destinationBucket);
                if (deleteCopiedFiles) s3Client.deleteObject(objectKey, sourceBucket);
            } catch (AmazonServiceException e) {
                logger.warn("Failed to move S3 object {}", objectKey, e);
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
            String sourcePrefix,
            ImmutableSet<String> allowedExtensions,
            Duration retentionPeriod,
            ImmutablePair<String, String> configuredTable
    ) {
        String tableKey = sourcePrefix.isEmpty() ?
                configuredTable.left + DELIMITER + configuredTable.right + DELIMITER :
                sourcePrefix + DELIMITER + configuredTable.left + DELIMITER + configuredTable.right + DELIMITER;
        logger.info("Listing files in S3 source location {} for table {}", sourceBucket, tableKey);
        return s3Client.getObjectsOlderThan(
                sourceBucket,
                tableKey,
                allowedExtensions,
                retentionPeriod,
                clock
        );
    }
}
