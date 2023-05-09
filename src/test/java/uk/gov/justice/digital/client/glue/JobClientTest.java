package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.GetJobRequest;
import com.amazonaws.services.glue.model.GetJobResult;
import com.amazonaws.services.glue.model.Job;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.JobProperties;

import javax.inject.Inject;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@MicronautTest
class JobClientTest {

    private static final String SPARK_JOB_NAME = "SomeTestJob";

    private static final AWSGlue mockClient = mock(AWSGlue.class);
    private static final GlueClientProvider mockClientProvider = mock(GlueClientProvider.class);
    private static final JobProperties mockJobProperties = mock(JobProperties.class);

    @Inject
    public JobClient underTest;

    @BeforeAll
    public static void setupMocks() {
        when(mockClientProvider.getClient()).thenReturn(mockClient);
        when(mockJobProperties.getSparkJobName()).thenReturn(SPARK_JOB_NAME);
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

    @MockBean(JobProperties.class)
    public JobProperties jobProperties() {
        return mockJobProperties;
    }

}