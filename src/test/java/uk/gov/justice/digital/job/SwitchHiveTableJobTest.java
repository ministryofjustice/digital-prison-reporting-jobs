package uk.gov.justice.digital.job;

import com.github.stefanbirkner.systemlambda.SystemLambda;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.HiveSchemaServiceException;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.HiveSchemaService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.SparkTestHelpers.containsTheSameElementsInOrderAs;

@ExtendWith(MockitoExtension.class)
public class SwitchHiveTableJobTest extends BaseSparkTest {

    private static final String TEST_CONFIG_KEY = "some-config-key";

    @Mock
    private ConfigService mockConfigService;
    @Mock
    private HiveSchemaService mockHiveSchemaService;
    @Mock
    private JobArguments mockJobArguments;
    @Captor
    private ArgumentCaptor<ImmutableSet<ImmutablePair<String, String>>> argumentCaptor;

    private SwitchHiveTableJob underTest;

    @BeforeEach
    public void setup() {
        reset(mockConfigService, mockHiveSchemaService, mockJobArguments);
        underTest = new SwitchHiveTableJob(mockConfigService, mockHiveSchemaService, mockJobArguments);
    }

    @Test
    public void shouldCompleteSuccessfullyWhenThereAreNoFailedTables() {
        Set<ImmutablePair<String, String>> expectedTables = new HashSet<>();
        expectedTables.add(new ImmutablePair<>("schema_1", "table_1"));
        expectedTables.add(new ImmutablePair<>("schema_1", "table_2"));
        expectedTables.add(new ImmutablePair<>("schema_2", "table_3"));

        when(mockJobArguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(ImmutableSet.copyOf(expectedTables));
        when(mockHiveSchemaService.switchPrisonsTableDataSource(argumentCaptor.capture())).thenReturn(Collections.emptySet());

        assertDoesNotThrow(() -> underTest.run());

        assertThat(argumentCaptor.getValue(), containsTheSameElementsInOrderAs(new ArrayList<>(expectedTables)));
    }

    @Test
    public void shouldFailWhenThereAreFailedTables() throws Exception {
        ImmutableSet<ImmutablePair<String, String>> failedTables = ImmutableSet.of(ImmutablePair.of("schema", "failed-table-1"));

        when(mockJobArguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(ImmutableSet.copyOf(failedTables));
        when(mockHiveSchemaService.switchPrisonsTableDataSource(any())).thenReturn(failedTables);

        SystemLambda.catchSystemExit(() -> underTest.run());
    }

    @Test
    public void shouldFailWhenSchemaServiceThrowsAnException() throws Exception {
        ImmutableSet<ImmutablePair<String, String>> table = ImmutableSet.of(ImmutablePair.of("schema_1", "table_1"));

        when(mockJobArguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(ImmutableSet.copyOf(table));
        when(mockHiveSchemaService.switchPrisonsTableDataSource(any())).thenThrow(new HiveSchemaServiceException("Schema service exception"));

        SystemLambda.catchSystemExit(() -> underTest.run());
    }

    @Test
    public void shouldFailWhenConfigServiceThrowsAnException() throws Exception {
        when(mockJobArguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenThrow(new RuntimeException("Config service error"));

        SystemLambda.catchSystemExit(() -> underTest.run());
    }
}