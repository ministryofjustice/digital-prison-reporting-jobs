package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.GetJobRequest;
import com.amazonaws.services.glue.model.GetJobResult;
import com.amazonaws.services.glue.model.Job;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@MicronautTest
class JobClientTest {

    private static final AWSGlue mockClient = mock(AWSGlue.class);

    // TODO - verify that this is safe
    private static final String SPARK_JOB_NAME_KEY = "spark.glue.JOB_NAME";
    private static final String SPARK_JOB_NAME = "SomeTestJob";

    @Inject
    public JobClient underTest;

    private static final GlueClientProvider mockClientProvider = mock(GlueClientProvider.class);

    @BeforeAll
    public static void setup() {
        System.setProperty(SPARK_JOB_NAME_KEY, SPARK_JOB_NAME);
        when(mockClientProvider.getClient()).thenReturn(mockClient);
    }

    @AfterAll
    public static void clearProperties() {
        System.clearProperty(SPARK_JOB_NAME_KEY);
    }

    @Test
    public void getJobParametersShouldReturnParametersConfiguredForJob() {
        val expectedRequest = new GetJobRequest().withJobName(SPARK_JOB_NAME);
        val fakeJobParameters = Collections.singletonMap("foo", "bar");
        val fakeJobResult = new GetJobResult().withJob(new Job().withDefaultArguments(fakeJobParameters));

        when(mockClient.getJob(eq(expectedRequest))).thenReturn(fakeJobResult);

        val result = underTest.getJobParameters();

        assertEquals(fakeJobParameters, result);
    }

    @MockBean(GlueClientProvider.class)
    public GlueClientProvider glueClientProvider() {
        return mockClientProvider;
    }

}