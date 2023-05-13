package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.GetJobRequest;
import com.amazonaws.services.glue.model.GetJobResult;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobProperties;


import java.util.Map;

@Singleton
public class JobClient {

    private static final Logger logger = LoggerFactory.getLogger(JobClient.class);

    private final AWSGlue glueClient;
    private final String jobName;

    @Inject
    public JobClient(GlueClientProvider glueClientProvider,
                     JobProperties jobProperties) {
        this.jobName = jobProperties.getSparkJobName();
        this.glueClient = glueClientProvider.getClient();
    }

    public Map<String, String> getJobParameters() {
        logger.info("Fetching job parameters");
        GetJobRequest request = new GetJobRequest().withJobName(jobName);
        GetJobResult result = glueClient.getJob(request);
        logger.info("Got result: " + result);
        return result.getJob().getDefaultArguments();
    }

}
