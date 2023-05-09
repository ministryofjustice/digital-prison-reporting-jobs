package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.ClientProvider;

@Singleton
public class GlueClientProvider implements ClientProvider<AWSGlue> {

    @Override
    public AWSGlue getClient() {
        return AWSGlueClientBuilder.defaultClient();
    }

}
