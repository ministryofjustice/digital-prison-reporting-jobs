package uk.gov.justice.digital.client.s3;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import jakarta.inject.Inject;
import lombok.Data;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import uk.gov.justice.digital.common.retry.RetryConfig;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Singleton;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.common.retry.RetryPolicyBuilder.buildRetryPolicy;

@Singleton
public class S3SchemaClient {

    private static final Logger logger = LoggerFactory.getLogger(S3SchemaClient.class);

    private final S3Client s3;
    private final RetryPolicy<S3SchemaResponse> retryPolicy;
    private final LoadingCache<String, S3SchemaResponse> cache;
    private final String contractRegistryName;
    private final Integer maxObjectsPerPage;

    static final String SCHEMA_FILE_EXTENSION = ".avsc";

    @Inject
    public S3SchemaClient(
            S3ClientProvider schemaClientProvider,
            JobArguments jobArguments
    ) {
        this.s3 = schemaClientProvider.getClient();
        RetryConfig retryConfig = new RetryConfig(jobArguments);
        this.retryPolicy = buildRetryPolicy(retryConfig, SdkClientException.class);
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(jobArguments.getSchemaCacheMaxSize())
                .expireAfterWrite(jobArguments.getSchemaCacheExpiryInMinutes(), TimeUnit.MINUTES)
                .build(createCacheLoader());
        this.contractRegistryName = jobArguments.getContractRegistryName();
        this.maxObjectsPerPage = jobArguments.getMaxObjectsPerPage();
    }

    public Optional<S3SchemaResponse> getSchema(String schemaName) {
        return getAvroSchema(schemaName + SCHEMA_FILE_EXTENSION);
    }

    public List<S3SchemaResponse> getAllSchemas(ImmutableSet<ImmutablePair<String, String>> schemaGroup) {
        List<S3SchemaResponse> schemas = new ArrayList<>();
        List<String> schemaKeys = listObjects();
        Set<String> schemaGroupWithExtension = schemaGroup.stream()
                .map(item -> item.getLeft() + "/" + item.getRight() + SCHEMA_FILE_EXTENSION)
                .collect(Collectors.toSet());

        for (String schemaKey : schemaKeys) {
            logger.debug("Processing schema key {}", schemaKey);
            if (schemaGroupWithExtension.contains(schemaKey)) {
                val schema = getAvroSchema(schemaKey).orElseThrow(() -> new RuntimeException("Failed to get schema " + schemaKey));
                schemas.add(schema);
            }
        }

        return schemas;
    }

    private Optional<S3SchemaResponse> getAvroSchema(String schemaNameWithExtension) {
        try {
            return Optional.ofNullable(cache.get(schemaNameWithExtension));
        } catch (UncheckedExecutionException | ExecutionException e) {
            logger.warn("Failed to retrieve schema {}", schemaNameWithExtension);
            return Optional.empty();
        }
    }

    private List<String> listObjects() throws SdkClientException {
        List<String> objectPaths = new LinkedList<>();

        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(contractRegistryName)
                .maxKeys(maxObjectsPerPage);

        ListObjectsV2Response objectList;
        do {
            objectList = s3.listObjectsV2(requestBuilder.build());
            objectList.contents().stream()
                    .filter(x -> x.key().toLowerCase().endsWith(SCHEMA_FILE_EXTENSION.toLowerCase()))
                    .forEach(x -> objectPaths.add(x.key()));
            requestBuilder.continuationToken(objectList.nextContinuationToken());
        } while (objectList.isTruncated().booleanValue());

        return objectPaths;
    }

    private @NotNull S3SchemaResponse getSchemaResponse(String schemaNameWithExtension) throws SdkClientException, IOException {
        logger.info("Getting schema for {}", schemaNameWithExtension);
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(contractRegistryName)
                .key(schemaNameWithExtension)
                .build();

        ResponseInputStream<GetObjectResponse> schemaObject = s3.getObject(request, ResponseTransformer.toInputStream());
        String schemaAvroString = IOUtils.toString(schemaObject, StandardCharsets.UTF_8);
        schemaObject.close();
        String versionId = schemaObject.response().versionId();
        return new S3SchemaResponse(schemaNameWithExtension.split("\\.")[0], schemaAvroString, versionId);
    }

    @NotNull
    private CacheLoader<String, S3SchemaResponse> createCacheLoader() {
        return new CacheLoader<String, S3SchemaResponse>() {
            @Override
            public S3SchemaResponse load(@NotNull String key) {
                return Failsafe.with(retryPolicy).get(() -> getSchemaResponse(key));
            }
        };
    }

    @Data
    public static class S3SchemaResponse {
        private final String id;
        private final String avro;
        private final String versionId;
    }
}
