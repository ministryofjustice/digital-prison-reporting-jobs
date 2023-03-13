package uk.gov.justice.digital.config;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import uk.gov.justice.digital.client.glue.JobClient;

import java.util.Map;
import java.util.Optional;

public class JobParameters {

    private final Map<String, String> config;

    public static JobParameters fromGlueJob() {
        return new JobParameters(JobClient.defaultClient().getJobParameters());
    }

    public JobParameters(Map<String, String> config) {
        this.config = config;
    }

    public String getAwsRegion() {
        return getMandatoryProperty("aws.region");
    }

    public String getAwsKinesisEndpointUrl() {
        return getMandatoryProperty("aws.kinesis.endpointUrl");
    }

    public String getKinesisReaderStreamName() {
        return getMandatoryProperty("kinesis.reader.streamName");
    }

    public Duration getKinesisReaderBatchDuration() {
        String durationSeconds = getMandatoryProperty("kinesis.reader.batchDurationSeconds");
        long parsedDuration = Long.parseLong(durationSeconds);
        return Durations.seconds(parsedDuration);
    }

    private String getMandatoryProperty(String jobParameter) {
        return Optional
            .ofNullable(config.get(jobParameter))
            .orElseThrow(() -> new IllegalStateException("Job Parameter: " + jobParameter + " is not set"));
    }

}
