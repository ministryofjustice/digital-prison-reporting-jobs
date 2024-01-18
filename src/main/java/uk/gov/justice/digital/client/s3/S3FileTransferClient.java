package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import javax.inject.Singleton;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

@Singleton
public class S3FileTransferClient {

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
                    objectPaths.add(summaryKey);
                }
            }
            request.setMarker(objectList.getMarker());
        } while (objectList.isTruncated());

        return objectPaths;
    }

    public void moveObject(String objectKey, String sourceBucket, String destinationBucket) {
        s3.copyObject(sourceBucket, objectKey, destinationBucket, objectKey);
        s3.deleteObject(sourceBucket, objectKey);
    }

    public void deleteObject(String objectKey, String sourceBucket) {
        s3.deleteObject(sourceBucket, objectKey);
    }
}
