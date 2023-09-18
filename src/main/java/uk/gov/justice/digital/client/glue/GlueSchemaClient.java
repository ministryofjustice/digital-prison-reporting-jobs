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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Optional;

@Singleton
public class GlueSchemaClient implements Serializable {

    private static final long serialVersionUID = 3954160928940543821L;

    private final GlueClientProvider glueClientProvider;

    // AWSGlue object requires special handling for Spark checkpointing since it is not serializable.
    // Transient ensures Java does not attempt to serialize it.
    // Volatile ensures it keeps the 'final' concurrency initialization guarantee.
    private transient volatile AWSGlue glueClient;
    private final String contractRegistryName;

    @Inject
    public GlueSchemaClient(GlueClientProvider glueClientProvider,
                            JobArguments jobArguments) {
        this.glueClientProvider = glueClientProvider;
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

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.glueClient = this.glueClientProvider.getClient();
    }

}
