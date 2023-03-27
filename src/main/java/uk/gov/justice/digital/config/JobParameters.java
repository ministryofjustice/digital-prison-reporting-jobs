package uk.gov.justice.digital.config;

import lombok.val;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.JobClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
public class JobParameters {

    private static final Logger logger = LoggerFactory.getLogger(JobParameters.class);

    private final Map<String, String> config;

    @Inject
    public JobParameters(JobClient jobClient) {
        this(jobClient.getJobParameters());
    }

    public JobParameters(Map<String, String> config) {
        this.config = config.entrySet()
            .stream()
            .map(this::cleanEntryKey)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        logger.info("Job initialised with parameters: {}", config);
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

    public Optional<String> getRawS3Path() {
        return getOptionalProperty("raw.s3.path");
    }

    public Optional<String> getStructuredS3Path() {
        return getOptionalProperty("structured.s3.path");
    }

    public Optional<String> getViolationsS3Path() {
        return getOptionalProperty("violations.s3.path");
    }

    private String getMandatoryProperty(String jobParameter) {
        return Optional
            .ofNullable(config.get(jobParameter))
            .orElseThrow(() -> new IllegalStateException("Job Parameter: " + jobParameter + " is not set"));
    }

    // TODO - consider supporting a default value where if no value is provided we throw an exception if there is no
    //        value at all
    private Optional<String> getOptionalProperty(String jobParameter) {
        return Optional
            .ofNullable(config.get(jobParameter));
    }

    // We expect job parameters to be specified with a leading -- prefix e.g. --some.job.setting consistent with how
    // AWS glue specifies job parameters. The prefix is removed to clean up code handling parameters by name.
    private Map.Entry<String, String> cleanEntryKey(Map.Entry<String, String> entry) {
        val cleanedKey = entry.getKey().replaceFirst("--", "");
        return new AbstractMap.SimpleEntry<>(cleanedKey, entry.getValue());
    }

}
