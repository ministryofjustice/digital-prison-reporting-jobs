package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.GetJobRequest;
import com.amazonaws.services.glue.model.GetJobResult;
import uk.gov.justice.digital.config.Properties;

import javax.inject.Singleton;
import java.util.Map;

@Singleton
public class JobClient {

    private final AWSGlue glueClient;
    private final String jobName;

    public JobClient() {
        this(AWSGlueClientBuilder.defaultClient());
    }

    private JobClient(AWSGlue client) {
        glueClient = client;
        jobName = Properties.getSparkJobName();
    }

    public Map<String, String> getJobParameters() {
        GetJobRequest request = new GetJobRequest().withJobName(jobName);
        GetJobResult result = glueClient.getJob(request);
        return result.getJob().getDefaultArguments();
    }

}
