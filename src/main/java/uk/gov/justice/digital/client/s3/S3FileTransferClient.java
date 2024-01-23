package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public S3FileTransferClient(S3ClientProvider s3ClientProvider) {
        this.s3 = s3ClientProvider.getClient();
    }

    public List<String> getObjectsOlderThan(String bucket, String extension, Long retentionDays, Clock clock) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket);
        return listObjects(extension, retentionDays, clock, request);
    }

    public List<String> getObjectsOlderThan(
            String bucket,
            String folder,
            String extension,
            Long retentionDays,
            Clock clock
    ) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket).withPrefix(folder);
        return listObjects(extension, retentionDays, clock, request);
    }

    private List<String> listObjects(String extension, Long retentionDays, Clock clock, ListObjectsRequest request) {
        LocalDateTime currentDate = LocalDateTime.now(clock);
        List<String> objectPaths = new LinkedList<>();
        ObjectListing objectList;
        do {
            objectList = s3.listObjects(request);
            for (S3ObjectSummary summary : objectList.getObjectSummaries()) {

                LocalDateTime lastModifiedDate = summary.getLastModified().toInstant().atZone(clock.getZone()).toLocalDateTime();
                boolean isBeforeRetentionPeriod = lastModifiedDate.isBefore(currentDate.minusDays(retentionDays));

                String summaryKey = summary.getKey();

                if (!summaryKey.endsWith(DELIMITER) && summaryKey.endsWith(extension) && isBeforeRetentionPeriod) {
                    logger.debug("Adding {}", summaryKey);
                    objectPaths.add(summaryKey);
                }
            }
            request.setMarker(objectList.getMarker());
        } while (objectList.isTruncated());

        return objectPaths;
    }

    public void copyObject(String objectKey, String sourceBucket, String destinationBucket) {
        logger.debug("Copying {}", objectKey);
        s3.copyObject(sourceBucket, objectKey, destinationBucket, objectKey);
    }

    public void deleteObject(String objectKey, String sourceBucket) {
        logger.debug("Deleting {}", objectKey);
        s3.deleteObject(sourceBucket, objectKey);
    }
}
