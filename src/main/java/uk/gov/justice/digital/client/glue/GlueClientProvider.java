package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.ClientProvider;

import java.io.Serializable;

@Singleton
public class GlueClientProvider implements ClientProvider<AWSGlue>, Serializable {

    private static final long serialVersionUID = -7516005122133494388L;

    @Override
    public AWSGlue getClient() {
        return AWSGlueClientBuilder.defaultClient();
    }

}
