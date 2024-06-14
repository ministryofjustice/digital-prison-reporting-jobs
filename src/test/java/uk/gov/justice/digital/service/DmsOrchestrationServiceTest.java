package uk.gov.justice.digital.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.dms.DmsClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DmsClientException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DmsOrchestrationServiceTest {

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private DmsClient mockDmsClient;

    private static final String TEST_TASK_ID = "test_task_id";
    private static final int WAIT_INTERVAL_SECONDS = 2;
    private static final int MAX_ATTEMPTS = 10;

    private DmsOrchestrationService underTest;

    @BeforeEach
    public void setup() {
        reset(mockJobArguments, mockDmsClient);

        underTest = new DmsOrchestrationService(mockJobArguments, mockDmsClient);
    }

    @Test
    void stopJobShouldStopDmsTaskWithGivenDomainName() {
        when(mockJobArguments.orchestrationWaitIntervalSeconds()).thenReturn(WAIT_INTERVAL_SECONDS);
        when(mockJobArguments.orchestrationMaxAttempts()).thenReturn(MAX_ATTEMPTS);

        underTest.stopTask(TEST_TASK_ID);

        verify(mockDmsClient, times(1)).stopTask(TEST_TASK_ID, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);
    }

    @Test
    void stopJobShouldFailWhenDmsClientThrowsAnException() {
        when(mockJobArguments.orchestrationWaitIntervalSeconds()).thenReturn(WAIT_INTERVAL_SECONDS);
        when(mockJobArguments.orchestrationMaxAttempts()).thenReturn(MAX_ATTEMPTS);

        doThrow(new DmsClientException("Client error")).when(mockDmsClient)
                .stopTask(TEST_TASK_ID, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        assertThrows(DmsClientException.class, () -> underTest.stopTask(TEST_TASK_ID));
    }
}
