package uk.gov.justice.digital.job;

import com.github.stefanbirkner.systemlambda.SystemLambda;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.exception.HiveSchemaServiceException;
import uk.gov.justice.digital.service.HiveSchemaService;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HiveTableCreationJobTest extends BaseSparkTest {

    @Mock
    private HiveSchemaService mockHiveSchemaService;

    private HiveTableCreationJob underTest;

    @BeforeEach
    public void setup() {
        underTest = new HiveTableCreationJob(mockHiveSchemaService);
    }

    @Test
    public void shouldCompleteSuccessfullyWhenThereAreNoFailedTables() {
        when(mockHiveSchemaService.replaceTables(any())).thenReturn(Collections.emptySet());

        assertDoesNotThrow(() -> underTest.run());
    }

    @Test
    public void shouldFailWhenThereAreFailedTables() throws Exception {
        Set<String> failedTables = Collections.singleton("failed-table-1");

        when(mockHiveSchemaService.replaceTables(any())).thenReturn(failedTables);

        SystemLambda.catchSystemExit(() -> underTest.run());
    }

    @Test
    public void shouldFailWhenSchemaServiceThrowsAnException() throws Exception {
        when(mockHiveSchemaService.replaceTables(any())).thenThrow(new HiveSchemaServiceException("Schema service exception"));

        SystemLambda.catchSystemExit(() -> underTest.run());
    }
}
