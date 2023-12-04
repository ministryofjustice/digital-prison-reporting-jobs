package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.Data;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;

import java.util.*;

@Singleton
public class GlueSchemaClient {

    private static final Logger logger = LoggerFactory.getLogger(GlueSchemaClient.class);

    private final AWSGlue glueClient;
    private final String contractRegistryName;
    // Glue schema registry allows a maximum of 1000 schema versions per registry. However, only 100 can be retrieved per sdk request
    // https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html#schema-registry-quotas
    private static final int MAX_SCHEMA_RESULTS = 100;

    @Inject
    public GlueSchemaClient(GlueClientProvider glueClientProvider,
                            JobArguments jobArguments) {
        this.glueClient = glueClientProvider.getClient();
        this.contractRegistryName = jobArguments.getContractRegistryName();
    }

    public Optional<GlueSchemaResponse> getSchema(String schemaName) {
        try {
            val request = createRequestForSchemaName(schemaName);
            val result = glueClient.getSchemaVersion(request);
            return Optional.of(new GlueSchemaResponse(
                    result.getSchemaVersionId(),
                    result.getSchemaDefinition(),
                    result.getVersionNumber()
            ));
        } catch (EntityNotFoundException e) {
            logger.warn("Schema not found for {}", schemaName);
            return Optional.empty();
        }
    }

    public List<GlueSchemaResponse> getAllSchemas(Set<String> schemaGroup) {
        List<GlueSchemaResponse> schemas = new ArrayList<>();
        ListSchemasRequest listSchemasRequest = createlistSchemasRequest();
        ListSchemasResult listSchemasResult;

        do {
            listSchemasResult = glueClient.listSchemas(listSchemasRequest);

            for (SchemaListItem schemaItem : listSchemasResult.getSchemas()) {
                String schemaName = schemaItem.getSchemaName();
                logger.info("Getting schema for {}", schemaName);
                if (schemaGroup.contains(schemaName)) {
                    val schema = getSchema(schemaName).orElseThrow(() -> new RuntimeException("Failed to get schema " + schemaName));
                    schemas.add(schema);
                }
            }
            listSchemasRequest.setNextToken(listSchemasResult.getNextToken());
        } while (listSchemasResult.getNextToken() != null);

        logger.info("Retrieved a total of {} schemas", schemas.size());

        return schemas;
    }

    private GetSchemaVersionRequest createRequestForSchemaName(String schemaName) {
        val schemaId = new SchemaId()
                .withRegistryName(contractRegistryName)
                .withSchemaName(schemaName);

        // Always request the latest version of the schema.
        val latestSchemaVersion = new SchemaVersionNumber().withLatestVersion(true);

        return new GetSchemaVersionRequest()
                .withSchemaId(schemaId)
                .withSchemaVersionNumber(latestSchemaVersion);
    }

    private ListSchemasRequest createlistSchemasRequest() {
        RegistryId registryId = new RegistryId().withRegistryName(contractRegistryName);
        return new ListSchemasRequest()
                .withRegistryId(registryId)
                .withMaxResults(MAX_SCHEMA_RESULTS);
    }

    @Data
    public static class GlueSchemaResponse {
        private final String id;
        private final String avro;
        private final Long versionNumber;
    }

}
