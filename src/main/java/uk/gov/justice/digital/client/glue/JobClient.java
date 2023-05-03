package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.GetJobRequest;
import com.amazonaws.services.glue.model.GetJobResult;
import io.micronaut.context.annotation.Bean;
import uk.gov.justice.digital.config.Properties;

import java.util.Map;
import java.util.Optional;

@Bean
public class JobClient {

    private static final String SPARK_JOB_NAME_PROPERTY = "spark.glue.JOB_NAME";

    private final AWSGlue glueClient;
    private final String jobName;

    public JobClient() {
        this(AWSGlueClientBuilder.defaultClient());
    }

    private JobClient(AWSGlue client) {
        glueClient = client;
        jobName = Properties.getSparkJobName();
    }

    public AWSGlue getGlueClient() {
        return glueClient;
    }

    public Map<String, String> getJobParameters() {
        GetJobRequest request = new GetJobRequest().withJobName(jobName);
        GetJobResult result = glueClient.getJob(request);
        return result.getJob().getDefaultArguments();
    }


}