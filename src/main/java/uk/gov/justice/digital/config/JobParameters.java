package uk.gov.justice.digital.config;

import lombok.val;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import uk.gov.justice.digital.client.glue.JobClient;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class JobParameters {

    private final Map<String, String> config;

    public static JobParameters fromGlueJob() {
        return new JobParameters(JobClient.defaultClient().getJobParameters());
    }

    public JobParameters(Map<String, String> config) {
        this.config = config.entrySet()
            .stream()
            .map(this::cleanEntryKey)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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

    // We expect job parameters to be specified with a leading -- prefix e.g. --some.job.setting consistent with how
    // AWS glue specifies job parameters. The prefix is removed to clean up code handling parameters by name.
    private Map.Entry<String, String> cleanEntryKey(Map.Entry<String, String> entry) {
        val cleanedKey = entry.getKey().replaceFirst("--", "");
        return new AbstractMap.SimpleEntry<>(cleanedKey, entry.getValue());
    }

}
