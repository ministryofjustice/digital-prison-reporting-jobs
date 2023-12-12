package uk.gov.justice.digital.client.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import jakarta.inject.Inject;
import lombok.Data;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class S3SchemaClient {

    private static final Logger logger = LoggerFactory.getLogger(S3SchemaClient.class);

    private final AmazonS3 s3;
    private final String contractRegistryName;

    static final String SCHEMA_FILE_EXTENSION = ".avsc";

    @Inject
    public S3SchemaClient(
            S3SchemaClientProvider schemaClientProvider,
            JobArguments jobArguments
    ) {
        this.s3 = schemaClientProvider.getClient();
        this.contractRegistryName = jobArguments.getContractRegistryName();
    }

    public Optional<S3SchemaResponse> getSchema(String schemaName) {
        return getAvroSchema(schemaName + SCHEMA_FILE_EXTENSION);
    }

    public List<S3SchemaResponse> getAllSchemas(Set<String> schemaGroup) {
        List<S3SchemaResponse> schemas = new ArrayList<>();
        List<String> schemaKeys = listObjects();
        Set<String> schemaGroupWithExtension = schemaGroup.stream()
                .map(item -> item + SCHEMA_FILE_EXTENSION)
                .collect(Collectors.toSet());

        for (String schemaKey : schemaKeys) {
            logger.info("Processing schema key {}", schemaKey);
            if (schemaGroupWithExtension.contains(schemaKey)) {
                val schema = getAvroSchema(schemaKey).orElseThrow(() -> new RuntimeException("Failed to get schema " + schemaKey));
                schemas.add(schema);
            }
        }

        return schemas;
    }

    private Optional<S3SchemaResponse> getAvroSchema(String schemaNameWithExtension) {
        try {
            return Optional.of(getSchemaResponse(schemaNameWithExtension));
        } catch (AmazonClientException | IOException e) {
            logger.warn("Failed to retrieve schema {}", schemaNameWithExtension);
            return Optional.empty();
        }
    }

    private List<String> listObjects() throws SdkClientException {
        List<String> objectPaths = new LinkedList<>();

        ListObjectsRequest request = new ListObjectsRequest().withBucketName(contractRegistryName);

        ObjectListing objectList;
        do {
            objectList = s3.listObjects(request);
            for (S3ObjectSummary summary : objectList.getObjectSummaries()) {
                String summaryKey = summary.getKey();
                if (summaryKey.endsWith(SCHEMA_FILE_EXTENSION)) {
                    objectPaths.add(summaryKey);
                }
            }
            request.setMarker(objectList.getMarker());
        } while (objectList.isTruncated());

        return objectPaths;
    }

    private @NotNull S3SchemaResponse getSchemaResponse(String schemaNameWithExtension) throws AmazonClientException, IOException {
        logger.info("Getting schema for {}", schemaNameWithExtension);
        S3Object schemaObject = s3.getObject(contractRegistryName, schemaNameWithExtension);
        S3ObjectInputStream schemaObjectInputStream = schemaObject.getObjectContent();
        try {
            String schemaAvroString = IOUtils.toString(schemaObjectInputStream);
            String versionId = schemaObject.getObjectMetadata().getVersionId();
            return new S3SchemaResponse(schemaNameWithExtension.split("\\.")[0], schemaAvroString, versionId);
        } finally {
            IOUtils.closeQuietly(schemaObjectInputStream, null);
            IOUtils.closeQuietly(schemaObject, null);
        }
    }

    @Data
    public static class S3SchemaResponse {
        private final String id;
        private final String avro;
        private final String versionId;
    }
}
