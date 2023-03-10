package uk.gov.justice.digital.config;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class ConfigTest {

    private static final String AWS_REGION_KEY = "aws.region";
    private static final String AWS_KINESIS_ENDPOINT_URL_KEY = "aws.kinesis.endpointUrl";
    private static final String JOB_NAME_KEY = "job.name";
    private static final String KINESIS_READER_STREAM_NAME_KEY = "kinesis.reader.streamName";
    private static final String KINESIS_READER_BATCH_DURATION_SECONDS_KEY = "kinesis.reader.batchDurationSeconds";

    private static final String AWS_REGION = "test-region";
    private static final String AWS_KINESIS_ENDPOINT_URL = "https://kinesis.example.com";
    private static final String JOB_NAME = "some-job";
    private static final String KINESIS_READER_STREAM_NAME = "some-kinesis-stream";
    private static final String KINESIS_READER_BATCH_DURATION_SECONDS = "5";

    @BeforeEach
    public void setProperties() {
        System.setProperty(AWS_REGION_KEY, AWS_REGION);
        System.setProperty(AWS_KINESIS_ENDPOINT_URL_KEY, AWS_KINESIS_ENDPOINT_URL);
        System.setProperty(JOB_NAME_KEY, JOB_NAME);
        System.setProperty(KINESIS_READER_STREAM_NAME_KEY, KINESIS_READER_STREAM_NAME);
        System.setProperty(KINESIS_READER_BATCH_DURATION_SECONDS_KEY, KINESIS_READER_BATCH_DURATION_SECONDS);
    }

    @AfterEach
    void clearProperties() {
        System.clearProperty(AWS_REGION_KEY);
        System.clearProperty(AWS_KINESIS_ENDPOINT_URL_KEY);
        System.clearProperty(KINESIS_READER_STREAM_NAME_KEY);
    }

    @Test
    public void shouldReturnAwsRegionWhenSet() {
        assertEquals(AWS_REGION, Config.getAwsRegion());
    }

    @Test
    public void shouldThrowExceptionWhenAwsRegionNotSet() {
        System.clearProperty(AWS_REGION_KEY);
        assertThrows(IllegalStateException.class, Config::getAwsRegion);
    }

    @Test
    public void shouldReturnAwsKinesisEndpointUrlWhenSet() {
        assertEquals(AWS_KINESIS_ENDPOINT_URL, Config.getAwsKinesisEndpointUrl());
    }

    @Test
    public void shouldThrowExceptionWhenKinesisEndpointUrlNotSet() {
        System.clearProperty(AWS_KINESIS_ENDPOINT_URL_KEY);
        assertThrows(IllegalStateException.class, Config::getAwsKinesisEndpointUrl);
    }

    @Test
    public void shouldReturnJobNameWhenSet() {
        assertEquals(JOB_NAME, Config.getJobName());
    }

    @Test
    public void shouldThrowExceptionWhenJobNameNotSet() {
        System.clearProperty(JOB_NAME_KEY);
        assertThrows(IllegalStateException.class, Config::getJobName);
    }

    @Test
    public void shouldReturnKinesisReaderStreamNameWhenSet() {
        assertEquals(KINESIS_READER_STREAM_NAME, Config.getKinesisReaderStreamName());
    }

    @Test
    public void shouldThrowExceptionWhenKinesisReaderStreamNameNotSet() {
        System.clearProperty(KINESIS_READER_STREAM_NAME_KEY);
        assertThrows(IllegalStateException.class, Config::getKinesisReaderStreamName);
    }

    @Test
    public void shouldReturnKinesisReaderBatchDurationWhenSet() {
        Duration expectedDuration = Durations.seconds(Long.parseLong(KINESIS_READER_BATCH_DURATION_SECONDS));
        assertEquals(expectedDuration, Config.getKinesisReaderBatchDuration());
    }

    @Test
    public void shouldThrowExceptionWhenKinesisReaderBatchDurationNotSet() {
        System.clearProperty(KINESIS_READER_BATCH_DURATION_SECONDS_KEY);
        assertThrows(IllegalStateException.class, Config::getKinesisReaderBatchDuration);
    }

    @Test
    public void shouldThrowExceptionWhenKinesisReaderBatchDurationInvalid() {
        System.setProperty(KINESIS_READER_BATCH_DURATION_SECONDS_KEY, "this is not a number");
        assertThrows(NumberFormatException.class, Config::getKinesisReaderBatchDuration);
    }
}