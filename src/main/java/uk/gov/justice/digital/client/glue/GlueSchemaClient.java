package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.Data;
import lombok.val;
import uk.gov.justice.digital.config.JobArguments;

import java.util.Optional;

@Singleton
public class GlueSchemaClient {

    private final AWSGlue glueClient;
    private final String contractRegistryName;

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
                    result.getSchemaDefinition()
            ));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
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

    @Data
    public static class GlueSchemaResponse {
        private final String id;
        private final String avro;
    }

}
