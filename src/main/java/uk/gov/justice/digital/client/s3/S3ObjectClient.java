package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;
import uk.gov.justice.digital.datahub.model.ListObjectsConfig;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    public List<FileLastModifiedDate> getObjectsOlderThan(
            String bucket,
            String folder,
            Pattern fileNameMatchRegex,
            Duration period,
            Clock clock
    ) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket).withPrefix(folder).withMaxKeys(maxObjectsPerPage);
        ListObjectsConfig listObjectConfig = new ListObjectsConfig(ListObjectsConfig.ObjectModifiedDateTime.EARLIER, period, clock);
        return listObjects(fileNameMatchRegex, listObjectConfig, clock, request);
    }

    public List<FileLastModifiedDate> getObjectsNewerThan(
            String bucket,
            String folder,
            Pattern fileNameMatchRegex,
            Duration period,
            Clock clock
    ) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket).withPrefix(folder).withMaxKeys(maxObjectsPerPage);
        ListObjectsConfig listObjectConfig = new ListObjectsConfig(ListObjectsConfig.ObjectModifiedDateTime.ON_OR_LATER, period, clock);
        return listObjects(fileNameMatchRegex, listObjectConfig, clock, request);
    }

    private List<FileLastModifiedDate> listObjects(Pattern fileNameMatchRegex, ListObjectsConfig config, Clock clock, ListObjectsRequest request) {
        List<FileLastModifiedDate> objectPaths = new LinkedList<>();
        ObjectListing objectList;
        do {
            objectList = s3.listObjects(request);
            List<S3ObjectSummary> objectSummaries = objectList.getObjectSummaries();
            logger.info("Listed a total of {} objects", objectSummaries.size());
            for (S3ObjectSummary summary : objectSummaries) {

                String summaryKey = summary.getKey();
                logger.debug("Listed {}", summaryKey);

                boolean fileNameMatches = fileNameMatchRegex.matcher(summaryKey).matches();
                LocalDateTime lastModifiedDateTime = summary.getLastModified().toInstant().atZone(clock.getZone()).toLocalDateTime();
                if (!summaryKey.endsWith(DELIMITER) && fileNameMatches) {
                    if (config.isWithinPeriod(lastModifiedDateTime)) {
                        logger.debug("Adding {}", summaryKey);
                        objectPaths.add(new FileLastModifiedDate(summaryKey, lastModifiedDateTime));
                    } else if (config.exitWhenOutsidePeriod()) {
                        // The CDC files are ordered by their modified date-time.
                        // When listing objects which occur later than the given date-time we can exit on encountering the first item with an earlier date-time
                        // This avoids searching though all the items.
                        logger.debug("Exiting on encountering file {} which is outside period of interest", summaryKey);
                        return objectPaths;
                    }
                }
            }
            request.setMarker(objectList.getNextMarker());
        } while (objectList.isTruncated());

        return objectPaths;
    }

    public void copyObject(String sourceKey, String destinationKey, String sourceBucket, String destinationBucket) {
        logger.info("Copying {} from {} to {}", sourceKey, sourceBucket, destinationBucket);
        s3.copyObject(sourceBucket, sourceKey, destinationBucket, destinationKey);
    }

    public Set<String> deleteObjects(List<String> objectKeys, String sourceBucket) {
        Set<String> successfullyDeletedObjects = new HashSet<>();
        int deletedObjectsCount = 0;

        List<List<String>> partitionedKeys = ListUtils.partition(objectKeys, maxObjectsPerPage);
        for (List<String> partition: partitionedKeys) {
            deletedObjectsCount = deletedObjectsCount + partition.size();
            logger.info("Deleting {} of {} objects", deletedObjectsCount, objectKeys.size());
            List<DeleteObjectsRequest.KeyVersion> objectKeyVersions = partition.stream()
                    .map(DeleteObjectsRequest.KeyVersion::new)
                    .collect(Collectors.toList());

            DeleteObjectsRequest request = new DeleteObjectsRequest(sourceBucket).withKeys(objectKeyVersions).withQuiet(true);
            Set<String> deletedObjects = s3.deleteObjects(request)
                    .getDeletedObjects()
                    .stream()
                    .map(DeleteObjectsResult.DeletedObject::getKey)
                    .collect(Collectors.toSet());

            successfullyDeletedObjects.addAll(deletedObjects);
        }

        Set<String> failedObjects = new HashSet<>(objectKeys);
        failedObjects.removeAll(successfullyDeletedObjects);
        return failedObjects;
    }
}
