package uk.gov.justice.digital.config;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

import java.util.Optional;

/**
 * Static Config class providing access to mandatory and optional config properties.
 *
 * Mandatory parameters will throw an IllegalStateException if the specified property is unset.
 *
 * Optional parameters will return an empty Optional if the specified property is unset.
 */
public class Config {

    public static String getAwsRegion() {
        return getMandatoryProperty("aws.region");
    }

    public static String getAwsKinesisEndpointUrl() {
        return getMandatoryProperty("aws.kinesis.endpointUrl");
    }

    public static String getJobName() {
        return getMandatoryProperty("job.name");
    }

    public static String getKinesisReaderStreamName() {
        return getMandatoryProperty("kinesis.reader.streamName");
    }

    public static Duration getKinesisReaderBatchDuration() {
        String durationSeconds = getMandatoryProperty("kinesis.reader.batchDurationSeconds");
        long parsedDuration = Long.parseLong(durationSeconds);
        return Durations.seconds(parsedDuration);
    }

    private static String getMandatoryProperty(String property) {
        return Optional
            .ofNullable(System.getProperty(property))
            .orElseThrow(() -> new IllegalStateException("Property: " + property + " is not set"));
    }

    // Prevent instantiation of this class.
    private Config() { }

}
