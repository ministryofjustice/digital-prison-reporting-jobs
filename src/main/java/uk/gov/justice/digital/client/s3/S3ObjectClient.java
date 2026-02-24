package uk.gov.justice.digital.client.s3;

import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import org.apache.commons.collections4.ListUtils;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Singleton
public class S3ObjectClient {

    private static final Logger logger = LoggerFactory.getLogger(S3ObjectClient.class);

    private final S3Client s3;
    private final Integer maxObjectsPerPage;
    public static final String DELIMITER = "/";

    @Inject
    public S3ObjectClient(S3ClientProvider s3ClientProvider, JobArguments jobArguments) {
        this.s3 = s3ClientProvider.getClient();
        this.maxObjectsPerPage = jobArguments.getMaxObjectsPerPage();
    }

    public String getObject(String bucket, String key) throws IOException {
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
        ResponseInputStream<GetObjectResponse> object = s3.getObject(request, ResponseTransformer.toInputStream());
        return IOUtils.toString(object, StandardCharsets.UTF_8);
    }

    public void saveObject(String bucket, String key, byte[] data, ContentType contentType) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentLength(Long.valueOf(data.length))
                .contentType(contentType.getMimeType())
                .build();

        s3.putObject(request, RequestBody.fromBytes(data));
    }

    public List<FileLastModifiedDate> getObjectsOlderThan(
            String bucket,
            String folder,
            Pattern fileNameMatchRegex,
            Duration period,
            Clock clock
    ) {
        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request
                .builder()
                .bucket(bucket)
                .prefix(folder)
                .maxKeys(maxObjectsPerPage);
        return listObjects(fileNameMatchRegex, period, clock, requestBuilder);
    }

    private List<FileLastModifiedDate> listObjects(Pattern fileNameMatchRegex, Duration period, Clock clock, ListObjectsV2Request.Builder requestBuilder) {
        List<FileLastModifiedDate> objectPaths = new LinkedList<>();
        ListObjectsV2Response objectList;

        do {
            objectList = s3.listObjectsV2(requestBuilder.build());
            logger.info("Listed a total of {} objects", objectList.keyCount());
            objectList.contents().stream()
                    .filter(content -> !content.key().endsWith(DELIMITER) && fileNameMatchRegex.matcher(content.key()).matches())
                    .filter(content -> content.lastModified().atZone(clock.getZone()).toLocalDateTime().isBefore(LocalDateTime.now(clock).minus(period)))
                    .forEach(content -> objectPaths.add(new FileLastModifiedDate(content.key(), content.lastModified().atZone(clock.getZone()).toLocalDateTime())));

            requestBuilder.continuationToken(objectList.nextContinuationToken());
        } while (objectList.isTruncated().booleanValue());

        return objectPaths;
    }

    public void copyObject(String sourceKey, String destinationKey, String sourceBucket, String destinationBucket) {
        logger.info("Copying {} from {} to {}", sourceKey, sourceBucket, destinationBucket);
        CopyObjectRequest request = CopyObjectRequest.builder()
                .sourceBucket(sourceBucket)
                .sourceKey(sourceKey)
                .destinationBucket(destinationBucket)
                .destinationKey(destinationKey)
                .build();

        s3.copyObject(request);
    }

    public Set<String> deleteObjects(List<String> objectKeys, String sourceBucket) {
        Set<String> failedObjects = new HashSet<>();
        int deletedObjectsCount = 0;

        List<List<String>> partitionedKeys = ListUtils.partition(objectKeys, maxObjectsPerPage);
        for (List<String> partition: partitionedKeys) {
            deletedObjectsCount = deletedObjectsCount + partition.size();
            logger.info("Deleting {} of {} objects", deletedObjectsCount, objectKeys.size());
            List<ObjectIdentifier> objectIds = partition.stream()
                    .map(key -> ObjectIdentifier.builder().key(key).build())
                    .toList();

            DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                    .bucket(sourceBucket)
                    .delete(delete -> delete.objects(objectIds).quiet(true))
                    .build();

            Set<String> failedDeletesInCurrentBatch = s3.deleteObjects(request)
                    .deleted()
                    .stream()
                    .map(DeletedObject::key)
                    .collect(Collectors.toSet());

            failedObjects.addAll(failedDeletesInCurrentBatch);
        }

        return failedObjects;
    }
}
