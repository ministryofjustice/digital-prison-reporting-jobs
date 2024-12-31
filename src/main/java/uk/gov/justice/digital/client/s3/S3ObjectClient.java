package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

@Singleton
public class S3ObjectClient {

    private static final Logger logger = LoggerFactory.getLogger(S3ObjectClient.class);

    private final AmazonS3 s3;
    private final Integer maxObjectsPerPage;
    public static final String DELIMITER = "/";

    @Inject
    public S3ObjectClient(S3ClientProvider s3ClientProvider, JobArguments jobArguments) {
        this.s3 = s3ClientProvider.getClient();
        this.maxObjectsPerPage = jobArguments.getMaxObjectsPerPage();
    }

    public List<String> getObjectsOlderThan(String bucket, Pattern fileNameRegex, Duration retentionPeriod, Clock clock) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket).withMaxKeys(maxObjectsPerPage);
        return listObjects(fileNameRegex, retentionPeriod, clock, request);
    }

    public List<String> getObjectsOlderThan(
            String bucket,
            String folder,
            Pattern fileNameMatchRegex,
            Duration retentionPeriod,
            Clock clock
    ) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket).withPrefix(folder).withMaxKeys(maxObjectsPerPage);
        return listObjects(fileNameMatchRegex, retentionPeriod, clock, request);
    }

    private List<String> listObjects(Pattern fileNameMatchRegex, Duration retentionPeriod, Clock clock, ListObjectsRequest request) {
        LocalDateTime currentDate = LocalDateTime.now(clock);
        List<String> objectPaths = new LinkedList<>();
        ObjectListing objectList;
        do {
            objectList = s3.listObjects(request);
            List<S3ObjectSummary> objectSummaries = objectList.getObjectSummaries();
            logger.info("Listed a total of {} objects", objectSummaries.size());
            for (S3ObjectSummary summary : objectSummaries) {

                LocalDateTime lastModifiedDate = summary.getLastModified().toInstant().atZone(clock.getZone()).toLocalDateTime();
                boolean isBeforeRetentionPeriod = lastModifiedDate.isBefore(currentDate.minus(retentionPeriod));

                String summaryKey = summary.getKey();
                logger.debug("Listed {}", summaryKey);

                boolean fileNameMatches = fileNameMatchRegex.matcher(summaryKey).matches();

                if (!summaryKey.endsWith(DELIMITER) && fileNameMatches && isBeforeRetentionPeriod) {
                    logger.debug("Adding {}", summaryKey);
                    objectPaths.add(summaryKey);
                }
            }
            request.setMarker(objectList.getNextMarker());
        } while (objectList.isTruncated());

        return objectPaths;
    }

    public void copyObject(String sourceKey, String destinationKey, String sourceBucket, String destinationBucket) {
        logger.info("Copying {}", sourceKey);
        s3.copyObject(sourceBucket, sourceKey, destinationBucket, destinationKey);
    }

    public void deleteObject(String objectKey, String sourceBucket) {
        logger.info("Deleting {}", objectKey);
        s3.deleteObject(sourceBucket, objectKey);
    }
}
