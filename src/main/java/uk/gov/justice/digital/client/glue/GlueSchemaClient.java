package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.config.JobArguments;

@Singleton
public class GlueSchemaClient {

    private final AWSGlue glueClient;
    private final JobArguments jobArguments;

    @Inject
    public GlueSchemaClient(GlueClientProvider glueClientProvider,
                            JobArguments jobArguments) {
       this.glueClient = glueClientProvider.getClient();
       this.jobArguments = jobArguments;
    }

    // TODO - determine what we need to pass in
    public void getSchema() {

    }
}
