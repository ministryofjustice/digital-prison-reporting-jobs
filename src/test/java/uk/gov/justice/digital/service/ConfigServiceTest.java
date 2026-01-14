package uk.gov.justice.digital.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.dms.DmsClient;
import uk.gov.justice.digital.client.s3.S3ConfigReaderClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.ConfigServiceException;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfigServiceTest {

    private static final String TEST_CONFIG_KEY = "some-config-key";

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private DmsClient mockDmsClient;
    @Mock
    private S3ConfigReaderClient mockS3ConfigClient;

    private ConfigService underTest;

    @BeforeEach
    void setup() {
        reset(mockJobArguments, mockDmsClient, mockS3ConfigClient);

        when(mockJobArguments.readConfigFromS3()).thenReturn(false);

        underTest = new ConfigService(mockJobArguments, mockDmsClient, mockS3ConfigClient);
    }

    @Test
    void shouldReturnConfiguredTables() throws JsonProcessingException {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        when(mockDmsClient.getReplicationTaskTables(TEST_CONFIG_KEY)).thenReturn(ImmutableSet.copyOf(configuredTables));

        Set<ImmutablePair<String, String>> result = underTest.getConfiguredTables(TEST_CONFIG_KEY);

        assertThat(result, containsInAnyOrder(configuredTables.toArray()));
    }

    @Test
    void shouldFailWhenThereAreNoConfiguredTables() throws JsonProcessingException {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.copyOf(Collections.emptySet());

        when(mockDmsClient.getReplicationTaskTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);

        assertThrows(ConfigServiceException.class, () -> underTest.getConfiguredTables(TEST_CONFIG_KEY));
    }

    @Test
    void shouldReturnConfiguredTablesFromS3WhenUseConfigFromS3IsEnabled() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        when(mockJobArguments.readConfigFromS3()).thenReturn(true);
        when(mockS3ConfigClient.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(ImmutableSet.copyOf(configuredTables));

        Set<ImmutablePair<String, String>> result = underTest.getConfiguredTables(TEST_CONFIG_KEY);

        assertThat(result, containsInAnyOrder(configuredTables.toArray()));
    }

    @Test
    void shouldFailWhenThereAreNoConfiguredTablesInS3WhenUseConfigFromS3IsEnabled() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.copyOf(Collections.emptySet());

        when(mockJobArguments.readConfigFromS3()).thenReturn(true);
        when(mockS3ConfigClient.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);

        assertThrows(ConfigServiceException.class, () -> underTest.getConfiguredTables(TEST_CONFIG_KEY));
    }
}
