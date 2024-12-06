package uk.gov.justice.digital.job;

import com.github.stefanbirkner.systemlambda.SystemLambda;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.ConfigServiceException;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.MaintenanceService;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class VacuumJobTest extends BaseSparkTest {

    @Mock
    private JobArguments arguments;
    @Mock
    private ConfigService configService;
    @Mock
    private MaintenanceService maintenanceService;
    @Captor
    ArgumentCaptor<String> deltaPathCaptor;

    private static final String ROOT_PATH = "s3://some-bucket";
    private static final String TEST_CONFIG_KEY = "test-config-key";
    private static final String DOMAIN_CONFIG_TABLE_1 = "source1/table1";
    private static final String DOMAIN_CONFIG_TABLE_2 = "source2/table2";
    private static final int RECURSE_MAX_DEPTH = 2;
    private VacuumJob underTest;

    @BeforeEach
    public void setupTest() {
        reset(arguments, configService, maintenanceService);
        underTest = new VacuumJob(maintenanceService, configService, sparkSessionProvider, arguments);
    }

    @Test
    void shouldVacuumConfiguredTables() {
        when(arguments.getMaintenanceTablesRootPath()).thenReturn(ROOT_PATH);
        when(arguments.getMaintenanceListTableRecurseMaxDepth()).thenReturn(RECURSE_MAX_DEPTH);
        when(arguments.getOptionalConfigKey()).thenReturn(Optional.of(TEST_CONFIG_KEY));
        when(configService.getConfiguredTablePaths(TEST_CONFIG_KEY)).thenReturn(ImmutableSet.of(DOMAIN_CONFIG_TABLE_1, DOMAIN_CONFIG_TABLE_2));
        doNothing().when(maintenanceService).vacuumDeltaTables(eq(spark), deltaPathCaptor.capture(), eq(0));

        List<String> expectedPaths = Arrays.asList(
                ROOT_PATH + "/" + DOMAIN_CONFIG_TABLE_1,
                ROOT_PATH + "/" + DOMAIN_CONFIG_TABLE_2
        );

        underTest.run();

        assertThat(deltaPathCaptor.getAllValues(), containsInAnyOrder(expectedPaths.toArray()));
    }

    @Test
    void shouldVacuumTablesInRecurseDepthOfRootPathWhenNoConfigIsProvided() {
        when(arguments.getMaintenanceTablesRootPath()).thenReturn(ROOT_PATH);
        when(arguments.getMaintenanceListTableRecurseMaxDepth()).thenReturn(RECURSE_MAX_DEPTH);
        when(arguments.getOptionalConfigKey()).thenReturn(Optional.empty());
        doNothing().when(maintenanceService).vacuumDeltaTables(eq(spark), any(), eq(RECURSE_MAX_DEPTH));

        underTest.run();

        verify(maintenanceService, times(1))
                .vacuumDeltaTables(spark, ROOT_PATH + "/", RECURSE_MAX_DEPTH);
    }

    @Test
    void shouldExitWithFailureStatusWhenConfigServiceThrowsAnException() throws Exception {
        when(arguments.getMaintenanceTablesRootPath()).thenReturn(ROOT_PATH);
        when(arguments.getMaintenanceListTableRecurseMaxDepth()).thenReturn(RECURSE_MAX_DEPTH);
        when(arguments.getOptionalConfigKey()).thenReturn(Optional.of(TEST_CONFIG_KEY));
        doThrow(new ConfigServiceException("config error")).when(configService).getConfiguredTablePaths(TEST_CONFIG_KEY);

        assertEquals(1, SystemLambda.catchSystemExit(() -> underTest.run()));

        verifyNoInteractions(maintenanceService);
    }

    @Test
    void shouldExitWithFailureStatusWhenConfigIsProvidedAndMaintenanceServiceThrowsAnException() throws Exception {
        when(arguments.getMaintenanceTablesRootPath()).thenReturn(ROOT_PATH);
        when(arguments.getMaintenanceListTableRecurseMaxDepth()).thenReturn(RECURSE_MAX_DEPTH);
        when(arguments.getOptionalConfigKey()).thenReturn(Optional.of(TEST_CONFIG_KEY));
        when(configService.getConfiguredTablePaths(TEST_CONFIG_KEY))
                .thenReturn(ImmutableSet.of(DOMAIN_CONFIG_TABLE_1, DOMAIN_CONFIG_TABLE_2));
        doThrow(new RuntimeException("Maintenance service exception"))
                .when(maintenanceService).vacuumDeltaTables(eq(spark), any(), eq(RECURSE_MAX_DEPTH));

        assertEquals(1, SystemLambda.catchSystemExit(() -> underTest.run()));
    }

    @Test
    void shouldExitWithFailureStatusWhenNoConfigIsProvidedAndMaintenanceServiceThrowsAnException() throws Exception {
        when(arguments.getMaintenanceTablesRootPath()).thenReturn(ROOT_PATH);
        when(arguments.getMaintenanceListTableRecurseMaxDepth()).thenReturn(RECURSE_MAX_DEPTH);
        when(arguments.getOptionalConfigKey()).thenReturn(Optional.empty());
        doThrow(new RuntimeException("Maintenance service exception"))
                .when(maintenanceService).vacuumDeltaTables(eq(spark), any(), eq(RECURSE_MAX_DEPTH));

        assertEquals(1, SystemLambda.catchSystemExit(() -> underTest.run()));
    }

}
