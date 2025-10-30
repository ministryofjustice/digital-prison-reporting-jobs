package uk.gov.justice.digital.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.collect.ImmutableSet;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.entity.ContentType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3ObjectClient;
import uk.gov.justice.digital.common.retry.RetryConfig;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.client.s3.S3ObjectClient.DELIMITER;
import static uk.gov.justice.digital.common.retry.RetryPolicyBuilder.buildRetryPolicy;

@Singleton
public class S3FileService {

    private static final Logger logger = LoggerFactory.getLogger(S3FileService.class);
    private final S3ObjectClient s3Client;
    private final Clock clock;
    private final Integer fileTransferParallelism;
    private final RetryPolicy<Void> voidRetryPolicy;
    private final RetryPolicy<List<FileLastModifiedDate>> fileListRetryPolicy;
    private final RetryPolicy<Set<String>> stringSetRetryPolicy;

    @Inject
    public S3FileService(
            S3ObjectClient s3Client,
            Clock clock,
            JobArguments jobArguments
    ) {
        this.s3Client = s3Client;
        this.clock = clock;
        this.fileTransferParallelism = jobArguments.fileTransferUseDefaultParallelism() ?
                Math.max(1, Runtime.getRuntime().availableProcessors() - 1) :
                jobArguments.getFileTransferParallelism();
        RetryConfig retryConfig = new RetryConfig(jobArguments);
        this.voidRetryPolicy = buildRetryPolicy(retryConfig, AmazonS3Exception.class);
        this.fileListRetryPolicy = buildRetryPolicy(retryConfig, AmazonS3Exception.class);
        this.stringSetRetryPolicy = buildRetryPolicy(retryConfig, AmazonS3Exception.class);
    }

    public List<FileLastModifiedDate> listFiles(String bucket, String sourcePrefix, Pattern fileNameMatchRegex, Duration retentionPeriod) {
        return Failsafe.with(fileListRetryPolicy).get(() -> s3Client.getObjectsOlderThan(bucket, sourcePrefix, fileNameMatchRegex, retentionPeriod, clock));
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

    public Set<String> copyObjects(
            List<String> objectKeys,
            String sourceBucket,
            String sourcePrefix,
            String destinationBucket,
            String destinationPrefix,
            boolean deleteCopiedFiles
    ) {
        ConcurrentHashMap<String, String> failedObjects = new ConcurrentHashMap<>();

        ExecutorService executor = Executors.newFixedThreadPool(fileTransferParallelism);
        CompletableFuture[] futures = objectKeys.stream()
                .map(objectKey -> CompletableFuture.runAsync(
                        () -> copyFunction(
                                sourceBucket,
                                sourcePrefix,
                                destinationBucket,
                                destinationPrefix,
                                failedObjects,
                                objectKey
                        ), executor)).toArray(CompletableFuture[]::new
                );

        CompletableFuture.allOf(futures).join();

        if (deleteCopiedFiles && objectKeys.size() != failedObjects.size()) {
            // Remove objects which failed to be copied from the final list to be deleted
            List<String> objectsKeysToDelete = objectKeys.stream()
                    .filter(x -> !failedObjects.contains(x))
                    .collect(Collectors.toList());
            Set<String> failedDeleteObjects = deleteObjects(objectsKeysToDelete, sourceBucket);
            failedDeleteObjects.forEach(object -> failedObjects.putIfAbsent(object, object));
        }

        return failedObjects.keySet();
    }

    public Set<String> deleteObjects(List<String> objectKeys, String sourceBucket) {
        try {
            return Failsafe.with(stringSetRetryPolicy).get(() -> s3Client.deleteObjects(objectKeys, sourceBucket));
        } catch (AmazonServiceException e) {
            logger.warn("Failed to delete S3 objects: {}", e.getErrorMessage());
            return new HashSet<>(objectKeys);
        }
    }

    public Set<String> getPreviousArchivedKeys(String sourceBucket, String configKey) {
        logger.info("Loading archived keys");
        String lastArchivedFilesPath = getLastArchivedFilesPath(configKey);
        try {
            return Failsafe.with(stringSetRetryPolicy).get(() -> Arrays
                    .stream(s3Client.getObject(sourceBucket, lastArchivedFilesPath).split("\n"))
                    .map(String::trim)
                    .collect(Collectors.toSet())
            );
        } catch (AmazonServiceException e) {
            logger.warn(
                    "Failed to get last archived keys from {}/{}. Returning empty set: {}",
                    sourceBucket,
                    lastArchivedFilesPath,
                    e.getErrorMessage()
            );
            return Collections.emptySet();
        }
    }

    public void saveArchivedKeys(String destinationBucket, String configKey, List<String> objectKeys) {
        if (!objectKeys.isEmpty()) {
            logger.info("Saving archived keys");
            String data = String.join("\n", objectKeys);
            String lastArchivedFilesPath = getLastArchivedFilesPath(configKey);
            try {
                byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
                Failsafe.with(voidRetryPolicy).run(() -> s3Client.saveObject(destinationBucket, lastArchivedFilesPath, dataBytes, ContentType.DEFAULT_TEXT));
            } catch (AmazonServiceException e) {
                logger.warn("Failed to save archived keys to {}/{}: {}", destinationBucket, lastArchivedFilesPath, e.getErrorMessage());
            }
        }
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
        return Failsafe.with(fileListRetryPolicy).get(() -> s3Client.getObjectsOlderThan(
                sourceBucket,
                tableKey,
                fileNameMatchRegex,
                period,
                clock
        ));
    }

    private void copyFunction(
            String sourceBucket,
            String sourcePrefix,
            String destinationBucket,
            String destinationPrefix,
            ConcurrentHashMap<String, String> failedObjects,
            String objectKey
    ) {
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
        } catch (AmazonServiceException e) {
            logger.warn("Failed to move S3 object {}", objectKey, e);
            failedObjects.putIfAbsent(objectKey, objectKey);
        }
    }

    @NotNull
    private static String getLastArchivedFilesPath(String configKey) {
        return String.format("last-archived-files/%s/archived.txt", configKey);
    }
}
