package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.GetJobRequest;
import com.amazonaws.services.glue.model.GetJobResult;
import com.amazonaws.services.glue.model.Job;
import io.micronaut.test.annotation.MockBean;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobProperties;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobClientTest {

    private static final String SPARK_JOB_NAME = "SomeTestJob";

    @Mock
    private AWSGlue mockClient = mock(AWSGlue.class);
    @Mock
    private GlueClientProvider mockClientProvider = mock(GlueClientProvider.class);
    @Mock
    private JobProperties mockJobProperties = mock(JobProperties.class);

    private JobClient underTest;

    @BeforeEach
    public void setupMocks() {
        when(mockClientProvider.getClient()).thenReturn(mockClient);
        when(mockJobProperties.getSparkJobName()).thenReturn(SPARK_JOB_NAME);
        underTest = new JobClient(mockClientProvider, mockJobProperties);
    }

    @Test
    public void getJobParametersShouldReturnParametersConfiguredForJob() {
        val expectedRequest = new GetJobRequest().withJobName(SPARK_JOB_NAME);
        val fakeJobParameters = Collections.singletonMap("foo", "bar");
        val fakeJobResult = new GetJobResult().withJob(new Job().withDefaultArguments(fakeJobParameters));

        when(mockClient.getJob(eq(expectedRequest))).thenReturn(fakeJobResult);

        val result = underTest.getJobParameters();

        assertEquals(fakeJobParameters, result);

        verify(mockClient, times(1)).getJob(eq(expectedRequest));
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