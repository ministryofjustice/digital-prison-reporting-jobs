package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import org.apache.commons.collections4.ListUtils;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
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

    public String getObject(String bucket, String key) {
        return s3.getObjectAsString(bucket, key);
    }

    public void saveObject(String bucket, String key, byte[] data, ContentType contentType) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(data.length);
        metadata.setContentType(contentType.getMimeType());
        ByteArrayInputStream stream = new ByteArrayInputStream(data);
        PutObjectRequest request = new PutObjectRequest(bucket, key, stream, metadata);
        s3.putObject(request);
    }

    public List<FileLastModifiedDate> getObjectsOlderThan(
            String bucket,
            String folder,
            Pattern fileNameMatchRegex,
            Duration period,
            Clock clock
    ) {
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(folder)
                .withMaxKeys(maxObjectsPerPage);
        return listObjects(fileNameMatchRegex, period, clock, request);
    }

    private List<FileLastModifiedDate> listObjects(Pattern fileNameMatchRegex, Duration period, Clock clock, ListObjectsV2Request request) {
        List<FileLastModifiedDate> objectPaths = new LinkedList<>();
        ListObjectsV2Result objectList;
        LocalDateTime upperTimeBound = LocalDateTime.now(clock).minus(period);
        do {
            objectList = s3.listObjectsV2(request);
            List<S3ObjectSummary> objectSummaries = objectList.getObjectSummaries();
            logger.info("Listed a total of {} objects", objectSummaries.size());
            for (S3ObjectSummary summary : objectSummaries) {
                String summaryKey = summary.getKey();
                logger.debug("Listed {}", summaryKey);

                boolean fileNameMatches = fileNameMatchRegex.matcher(summaryKey).matches();
                LocalDateTime lastModifiedDateTime = summary.getLastModified().toInstant().atZone(clock.getZone()).toLocalDateTime();
                boolean modifiedBeforeCurrentDateTime = lastModifiedDateTime.isBefore(upperTimeBound);
                if (!summaryKey.endsWith(DELIMITER) && fileNameMatches && modifiedBeforeCurrentDateTime) {
                    logger.debug("Adding {}", summaryKey);
                    objectPaths.add(new FileLastModifiedDate(summaryKey, lastModifiedDateTime));
                }
            }
            request.setContinuationToken(objectList.getNextContinuationToken());
        } while (objectList.isTruncated());

        return objectPaths;
    }

    public void copyObject(String sourceKey, String destinationKey, String sourceBucket, String destinationBucket) {
        logger.info("Copying {} from {} to {}", sourceKey, sourceBucket, destinationBucket);
        s3.copyObject(sourceBucket, sourceKey, destinationBucket, destinationKey);
    }

    public Set<String> deleteObjects(List<String> objectKeys, String sourceBucket) {
        Set<String> failedObjects = new HashSet<>();
        int deletedObjectsCount = 0;

        List<List<String>> partitionedKeys = ListUtils.partition(objectKeys, maxObjectsPerPage);
        for (List<String> partition: partitionedKeys) {
            deletedObjectsCount = deletedObjectsCount + partition.size();
            logger.info("Deleting {} of {} objects", deletedObjectsCount, objectKeys.size());
            List<DeleteObjectsRequest.KeyVersion> objectKeyVersions = partition.stream()
                    .map(DeleteObjectsRequest.KeyVersion::new)
                    .toList();

            DeleteObjectsRequest request = new DeleteObjectsRequest(sourceBucket).withKeys(objectKeyVersions).withQuiet(true);
            Set<String> failedDeletesInCurrentBatch = s3.deleteObjects(request)
                    .getDeletedObjects()
                    .stream()
                    .map(DeleteObjectsResult.DeletedObject::getKey)
                    .collect(Collectors.toSet());

            failedObjects.addAll(failedDeletesInCurrentBatch);
        }

        return failedObjects;
    }
}
