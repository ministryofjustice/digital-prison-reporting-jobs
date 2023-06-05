package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetSchemaVersionRequest;
import com.amazonaws.services.glue.model.SchemaId;
import com.amazonaws.services.glue.model.SchemaVersionNumber;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;

import java.util.Optional;

@Singleton
public class GlueSchemaClient {

    private final AWSGlue glueClient;
    private final String contractRegistryName;
    private static final Logger logger = LoggerFactory.getLogger(GlueSchemaClient.class);

    @Inject
    public GlueSchemaClient(GlueClientProvider glueClientProvider,
                            JobArguments jobArguments) {
        this.glueClient = glueClientProvider.getClient();
        this.contractRegistryName = jobArguments.getContractRegistryName();
    }

    public Optional<String> getSchema(String schemaName) {
        try {
            val result = glueClient.getSchemaVersion(createRequestForSchemaName(schemaName));
            val schemaData = result.getSchemaDefinition();
            return Optional.of(schemaData);
        }
        catch (EntityNotFoundException e) {
            logger.error("Failed to retrieve schema" + e);
            return Optional.empty();
        }
    }

    private GetSchemaVersionRequest createRequestForSchemaName(String schemaName) {
        val schemaId = new SchemaId()
                .withRegistryName(contractRegistryName)
                .withSchemaName(schemaName);

        val latestSchemaVersion = new SchemaVersionNumber()
                .withLatestVersion(true);

        return new GetSchemaVersionRequest()
                .withSchemaId(schemaId)
                .withSchemaVersionNumber(latestSchemaVersion);
    }

}
