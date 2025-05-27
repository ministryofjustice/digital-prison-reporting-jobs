package uk.gov.justice.digital.job;

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DmsOrchestrationServiceException;
import uk.gov.justice.digital.service.DmsOrchestrationService;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doNothing;

@ExtendWith(MockitoExtension.class)
class UpdateDmsCdcTaskStartTimeJobTest {

    @Mock
    JobArguments mockJobArguments;
    @Mock
    DmsOrchestrationService mockDmsOrchestrationService;

    private UpdateDmsCdcTaskStartTimeJob underTest;

    @BeforeEach
    void setup() {
        reset(mockDmsOrchestrationService, mockJobArguments);

        underTest = new UpdateDmsCdcTaskStartTimeJob(mockDmsOrchestrationService, mockJobArguments);
    }

    @Test
    void shouldUpdateTheStartTimeOfConfiguredDmsTask() {
        String fullLoadTaskId = "dms-task-id";
        String cdcTaskId = "cdc-dms-task-id";

        when(mockJobArguments.getDmsTaskId()).thenReturn(fullLoadTaskId);
        when(mockJobArguments.getCdcDmsTaskId()).thenReturn(cdcTaskId);
        doNothing().when(mockDmsOrchestrationService).updateCdcTaskStartTime(fullLoadTaskId, cdcTaskId);

        assertDoesNotThrow(() -> underTest.run());
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    @SuppressWarnings("java:S2699")
    void shouldExitWithFailureStatusWhenDmsOrchestrationServiceThrowsAnException() {
        String fullLoadTaskId = "dms-task-id";
        String cdcTaskId = "cdc-dms-task-id";

        when(mockJobArguments.getDmsTaskId()).thenReturn(fullLoadTaskId);
        when(mockJobArguments.getCdcDmsTaskId()).thenReturn(cdcTaskId);
        doThrow(new DmsOrchestrationServiceException("service error")).when(mockDmsOrchestrationService).updateCdcTaskStartTime(fullLoadTaskId, cdcTaskId);

        underTest.run();
    }
}
