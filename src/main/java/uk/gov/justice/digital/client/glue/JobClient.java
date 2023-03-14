package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.GetJobRequest;
import com.amazonaws.services.glue.model.GetJobResult;

import java.util.Map;
import java.util.Optional;

public class JobClient {

    private static final String SPARK_JOB_NAME_PROPERTY = "spark.glue.JOB_NAME";

    private final AWSGlue glueClient;
    private final String jobName;

    public static JobClient defaultClient() {
        return new JobClient(AWSGlueClientBuilder.defaultClient());
    }

    private JobClient(AWSGlue client) {
        glueClient = client;
        jobName = getSparkJobName();
    }

    public Map<String, String> getJobParameters() {
        GetJobRequest request = new GetJobRequest().withJobName(jobName);
        GetJobResult result = glueClient.getJob(request);
        return result.getJob().getDefaultArguments();
    }

    private static String getSparkJobName() {
        return Optional
            .ofNullable(System.getProperty(SPARK_JOB_NAME_PROPERTY))
            .orElseThrow(() -> new IllegalStateException("Property " + SPARK_JOB_NAME_PROPERTY + " not set"));
    }

}
