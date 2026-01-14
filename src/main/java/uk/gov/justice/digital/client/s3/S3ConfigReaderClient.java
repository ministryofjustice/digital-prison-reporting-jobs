package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.ConfigReaderClientException;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class S3ConfigReaderClient {

    private static final Logger logger = LoggerFactory.getLogger(S3ConfigReaderClient.class);

    private final AmazonS3 s3;

    private final String configBucketName;

    static final String CONFIGS_PATH = "configs/";

    static final String CONFIG_FILE_SUFFIX = "/table-config.json";

    @Inject
    public S3ConfigReaderClient(
            S3ClientProvider clientProvider,
            JobArguments jobArguments
    ) {
        this.s3 = clientProvider.getClient();
        this.configBucketName = jobArguments.getConfigS3Bucket();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public ImmutableSet<ImmutablePair<String, String>> getConfiguredTables(String configKey) {
        String configFileKey = CONFIGS_PATH + configKey + CONFIG_FILE_SUFFIX;
        logger.info("Loading config with key: {} from location: {}", configKey, configFileKey);
        try {
            String configString = s3.getObjectAsString(configBucketName, configFileKey);
            val config = new ObjectMapper().readValue(configString, HashMap.class);
            return ImmutableSet.copyOf(convertToImmutablePairs((ArrayList<String>) config.get("tables")));
        } catch (Exception e) {
            throw new ConfigReaderClientException("Exception when loading config " + configFileKey, e);
        }
    }

    @NotNull
    private static List<ImmutablePair<String, String>> convertToImmutablePairs(ArrayList<String> strings) {
        return strings.stream().map(str -> {
            String[] split = str.split("/");
            validate(str, split);

            String schema = split[0];
            String table = split[1];
            return ImmutablePair.of(schema, table);
        }).toList();
    }

    private static void validate(String str, String[] split) {
        if (split.length != 2) {
            String errorMessage = String.format("Config %s does not match format schema/table", str);
            throw new ConfigReaderClientException(errorMessage);
        }
    }
}
