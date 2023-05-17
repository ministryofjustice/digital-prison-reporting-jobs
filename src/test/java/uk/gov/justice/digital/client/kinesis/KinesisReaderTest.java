package uk.gov.justice.digital.client.kinesis;


import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.config.JobProperties;

import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KinesisReaderTest {

    @Mock
    VoidFunction<JavaRDD<byte[]>> batchProcessor;
    private KinesisReader reader;

    @BeforeEach
    void setUp() {
        reader = mock(KinesisReader.class);
    }

    @Test
    void shouldTestConstructorObjectCreate() {
        val jobParameters = spy(new JobParameters(Collections.emptyMap()));
        val jobProperties = spy(new JobProperties());
        System.out.println(jobProperties);
        doReturn("test").when(jobProperties).getSparkJobName();
        doReturn(Durations.seconds(1000)).when(jobParameters).getKinesisReaderBatchDuration();
        doReturn("aws://kinesis-endpoint").when(jobParameters).getAwsKinesisEndpointUrl();
        doReturn("eu-west-1").when(jobParameters).getAwsRegion();
        doReturn("test-stream").when(jobParameters).getKinesisReaderStreamName();
        val sparkConf = new SparkConf().setMaster("local").setAppName("test");
        KinesisReader testReader = new KinesisReader(jobParameters, jobProperties, sparkConf);
        assertNotNull(testReader);
    }

    @Test
    void shouldTestSetBatchProcessor() {
        reader.setBatchProcessor(batchProcessor);
        verify(reader, atLeastOnce()).setBatchProcessor(any());
    }

    @Test
    void shouldTestStartAndAwaitTermination() throws InterruptedException {
        reader.startAndAwaitTermination();
        verify(reader, atLeastOnce()).startAndAwaitTermination();
    }
}