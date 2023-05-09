package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.ClientProvider;

@Singleton
public class GlueClientProvider implements ClientProvider<AWSGlue> {

    @Override
    public AWSGlue getClient() {
        return null;
    }

}
