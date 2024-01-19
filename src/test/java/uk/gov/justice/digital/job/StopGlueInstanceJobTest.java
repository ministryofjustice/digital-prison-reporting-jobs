package uk.gov.justice.digital.job;

import com.github.stefanbirkner.systemlambda.SystemLambda;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.GlueClientException;
import uk.gov.justice.digital.service.GlueOrchestrationService;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class StopGlueInstanceJobTest extends BaseSparkTest {

    @Mock
    GlueOrchestrationService mockGlueOrchestrationService;
    @Mock
    JobArguments mockJobArguments;

    private static final String TEST_JOB_NAME = "test_glue_job";

    private StopGlueInstanceJob underTest;

    @BeforeEach
    public void setup() {
        reset(mockGlueOrchestrationService, mockJobArguments);

        underTest = new StopGlueInstanceJob(mockGlueOrchestrationService, mockJobArguments);
    }

    @Test
    public void shouldStopGlueJobWithGivenName() {
        when(mockJobArguments.getStopGlueInstanceJobName()).thenReturn(TEST_JOB_NAME);

        underTest.run();

        verify(mockGlueOrchestrationService, times(1)).stopJob(TEST_JOB_NAME);
    }

    @Test
    public void shouldFailWhenAnExceptionOccursInService() throws Exception {
        when(mockJobArguments.getStopGlueInstanceJobName()).thenReturn(TEST_JOB_NAME);
        doThrow(new GlueClientException("error")).when(mockGlueOrchestrationService).stopJob(any());

        SystemLambda.catchSystemExit(() -> underTest.run());
    }
}
