package uk.gov.justice.digital.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.collect.ImmutableSet;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3ObjectClient;
import uk.gov.justice.digital.common.retry.RetryConfig;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.client.s3.S3ObjectClient.DELIMITER;
import static uk.gov.justice.digital.common.retry.RetryPolicyBuilder.buildRetryPolicy;

@Singleton
public class S3FileService {

    private static final Logger logger = LoggerFactory.getLogger(S3FileService.class);
    private final S3ObjectClient s3Client;
    private final Clock clock;
    private final RetryPolicy<Void> voidRetryPolicy;
    private final RetryPolicy<List<FileLastModifiedDate>> retryPolicy;

    @Inject
    public S3FileService(
            S3ObjectClient s3Client,
            Clock clock,
            JobArguments jobArguments
    ) {
        this.s3Client = s3Client;
        this.clock = clock;
        RetryConfig retryConfig = new RetryConfig(jobArguments);
        this.voidRetryPolicy = buildRetryPolicy(retryConfig, AmazonS3Exception.class);
        this.retryPolicy = buildRetryPolicy(retryConfig, AmazonS3Exception.class);
    }

    public List<FileLastModifiedDate> listFiles(String bucket, String sourcePrefix, Pattern fileNameMatchRegex, Duration retentionPeriod) {
        return Failsafe.with(retryPolicy).get(() -> s3Client.getObjectsOlderThan(bucket, sourcePrefix, fileNameMatchRegex, retentionPeriod, clock));
    }

    public List<FileLastModifiedDate> listFilesBeforePeriod(
            String sourceBucket,
            String sourcePrefix,
            ImmutableSet<ImmutablePair<String, String>> configuredTables,
            Pattern fileNameMatchRegex,
            Duration period
    ) {
        return configuredTables.stream()
                .flatMap(configuredTable -> listFilesBeforePeriod(sourceBucket, sourcePrefix, fileNameMatchRegex, period, configuredTable).stream())
                .collect(Collectors.toList());
    }

    public List<FileLastModifiedDate> listFilesAfterPeriod(
            String sourceBucket,
            String sourcePrefix,
            ImmutableSet<ImmutablePair<String, String>> configuredTables,
            Pattern fileNameMatchRegex,
            Duration period
    ) {
        return configuredTables.stream()
                .flatMap(configuredTable -> listFilesAfterPeriod(sourceBucket, sourcePrefix, fileNameMatchRegex, period, configuredTable).stream())
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

                Failsafe.with(voidRetryPolicy).run(() -> s3Client.copyObject(objectKey, destinationKey, sourceBucket, destinationBucket));
                if (deleteCopiedFiles) Failsafe.with(voidRetryPolicy).run(() -> s3Client.deleteObject(objectKey, sourceBucket));
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
                Failsafe.with(voidRetryPolicy).run(() -> s3Client.deleteObject(objectKey, sourceBucket));
            } catch (AmazonServiceException e) {
                logger.warn("Failed to delete S3 object {}: {}", objectKey, e.getErrorMessage());
                failedObjects.add(objectKey);
            }
        }

        return failedObjects;
    }

    private List<FileLastModifiedDate> listFilesBeforePeriod(
            String sourceBucket,
            String sourcePrefix,
            Pattern fileNameMatchRegex,
            Duration period,
            ImmutablePair<String, String> configuredTable
    ) {
        String tableKey = sourcePrefix.isEmpty() ?
                configuredTable.left + DELIMITER + configuredTable.right + DELIMITER :
                sourcePrefix + DELIMITER + configuredTable.left + DELIMITER + configuredTable.right + DELIMITER;
        logger.info("Listing files before current time - {} in S3 source location {} for table {}", period, sourceBucket, tableKey);
        return Failsafe.with(retryPolicy).get(() -> s3Client.getObjectsOlderThan(
                sourceBucket,
                tableKey,
                fileNameMatchRegex,
                period,
                clock
        ));
    }

    private List<FileLastModifiedDate> listFilesAfterPeriod(
            String sourceBucket,
            String sourcePrefix,
            Pattern fileNameMatchRegex,
            Duration period,
            ImmutablePair<String, String> configuredTable
    ) {
        String tableKey = sourcePrefix.isEmpty() ?
                configuredTable.left + DELIMITER + configuredTable.right + DELIMITER :
                sourcePrefix + DELIMITER + configuredTable.left + DELIMITER + configuredTable.right + DELIMITER;
        logger.info("Listing files after current time - {} in S3 source location {} for table {}", period, sourceBucket, tableKey);
        return Failsafe.with(retryPolicy).get(() -> s3Client.getObjectsNewerThan(
                sourceBucket,
                tableKey,
                fileNameMatchRegex,
                period,
                clock
        ));
    }
}
