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
    private DmsClient mockDmsClient;

    private ConfigService underTest;

    @BeforeEach
    void setup() {
        reset(mockDmsClient);

        underTest = new ConfigService(mockDmsClient);
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
}
