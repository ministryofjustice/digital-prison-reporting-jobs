package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

@Singleton
public class S3FileTransferClient {

    private static final Logger logger = LoggerFactory.getLogger(S3FileTransferClient.class);

    private final AmazonS3 s3;
    public static final String DELIMITER = "/";

    @Inject
    public S3FileTransferClient(S3ClientProvider s3ClientProvider) {
        this.s3 = s3ClientProvider.getClient();
    }

    public List<String> getObjectsOlderThan(String bucket, ImmutableSet<String> allowedExtensions, Long retentionDays, Clock clock) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket);
        return listObjects(allowedExtensions, retentionDays, clock, request);
    }

    public List<String> getObjectsOlderThan(
            String bucket,
            String folder,
            ImmutableSet<String> allowedExtensions,
            Long retentionDays,
            Clock clock
    ) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket).withPrefix(folder);
        return listObjects(allowedExtensions, retentionDays, clock, request);
    }

    private List<String> listObjects(ImmutableSet<String> allowedExtensions, Long retentionDays, Clock clock, ListObjectsRequest request) {
        LocalDateTime currentDate = LocalDateTime.now(clock);
        List<String> objectPaths = new LinkedList<>();
        ObjectListing objectList;
        do {
            objectList = s3.listObjects(request);
            for (S3ObjectSummary summary : objectList.getObjectSummaries()) {

                LocalDateTime lastModifiedDate = summary.getLastModified().toInstant().atZone(clock.getZone()).toLocalDateTime();
                boolean isBeforeRetentionPeriod = lastModifiedDate.isBefore(currentDate.minusDays(retentionDays));

                String summaryKey = summary.getKey();
                logger.debug("Listed {}", summaryKey);

                boolean extensionAllowed = allowedExtensions.contains("*") || allowedExtensions.contains(getFileExtension(summaryKey));

                if (!summaryKey.endsWith(DELIMITER) && extensionAllowed && isBeforeRetentionPeriod) {
                    logger.debug("Adding {}", summaryKey);
                    objectPaths.add(summaryKey);
                }
            }
            request.setMarker(objectList.getMarker());
        } while (objectList.isTruncated());

        return objectPaths;
    }

    public void copyObject(String objectKey, String sourceBucket, String destinationBucket) {
        logger.info("Copying {}", objectKey);
        s3.copyObject(sourceBucket, objectKey, destinationBucket, objectKey);
    }

    public void deleteObject(String objectKey, String sourceBucket) {
        logger.info("Deleting {}", objectKey);
        s3.deleteObject(sourceBucket, objectKey);
    }

    private static String getFileExtension(String summaryKey) {
        String[] splits = summaryKey.split("\\.");
        return "." + splits[splits.length - 1].toLowerCase();
    }
}
