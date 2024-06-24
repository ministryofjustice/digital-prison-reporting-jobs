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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;

@ExtendWith(MockitoExtension.class)
public class GlueTriggerActivationJobTest extends BaseSparkTest {

    @Mock
    GlueOrchestrationService mockGlueOrchestrationService;
    @Mock
    JobArguments mockJobArguments;

    private static final String TRIGGER_NAME = "some-trigger-name";

    private GlueTriggerActivationJob underTest;

    @BeforeEach
    public void setup() {
        reset(mockGlueOrchestrationService, mockJobArguments);

        underTest = new GlueTriggerActivationJob(mockGlueOrchestrationService, mockJobArguments);
    }

    @Test
    void shouldActivateGlueTriggerWithGivenNameWhenActivateTriggerArgumentIsTrue() {
        when(mockJobArguments.getGlueTriggerName()).thenReturn(TRIGGER_NAME);
        when(mockJobArguments.activateGlueTrigger()).thenReturn(true);

        underTest.run();

        verify(mockGlueOrchestrationService, times(1)).activateTrigger(TRIGGER_NAME);
    }

    @Test
    void shouldDeactivateGlueTriggerWithGivenNameWhenActivateTriggerArgumentIsFalse() {
        when(mockJobArguments.getGlueTriggerName()).thenReturn(TRIGGER_NAME);
        when(mockJobArguments.activateGlueTrigger()).thenReturn(false);

        underTest.run();

        verify(mockGlueOrchestrationService, times(1)).deactivateTrigger(TRIGGER_NAME);
    }

    @Test
    void shouldFailWhenAnExceptionOccursInServiceWhileActivatingTrigger() throws Exception {
        when(mockJobArguments.getGlueTriggerName()).thenReturn(TRIGGER_NAME);
        when(mockJobArguments.activateGlueTrigger()).thenReturn(true);
        doThrow(new GlueClientException("error")).when(mockGlueOrchestrationService).activateTrigger(any());

        assertEquals(1, SystemLambda.catchSystemExit(() -> underTest.run()));
    }

    @Test
    void shouldFailWhenAnExceptionOccursInServiceWhileDeactivatingTrigger() throws Exception {
        when(mockJobArguments.getGlueTriggerName()).thenReturn(TRIGGER_NAME);
        when(mockJobArguments.activateGlueTrigger()).thenReturn(false);
        doThrow(new GlueClientException("error")).when(mockGlueOrchestrationService).deactivateTrigger(any());

        assertEquals(1, SystemLambda.catchSystemExit(() -> underTest.run()));
    }
}
