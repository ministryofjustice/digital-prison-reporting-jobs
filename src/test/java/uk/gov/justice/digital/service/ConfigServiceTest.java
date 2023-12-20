package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3ConfigReaderClient;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfigServiceTest {

    private static final String TEST_CONFIG_KEY = "some-config-key";

    @Mock
    private S3ConfigReaderClient mockConfigClient;

    private ConfigService underTest;

    @BeforeEach
    public void setup() {
        reset(mockConfigClient);

        underTest = new ConfigService(mockConfigClient);
    }

    @Test
    public void shouldReturnConfiguredTables() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        when(mockConfigClient.getConfiguredTables(eq(TEST_CONFIG_KEY))).thenReturn(ImmutableSet.copyOf(configuredTables));

        Set<ImmutablePair<String, String>> result = underTest.getConfiguredTables(TEST_CONFIG_KEY);

        assertThat(result, containsInAnyOrder(configuredTables.toArray()));
    }

    @Test
    public void shouldFailWhenThereAreNoConfiguredTables() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.copyOf(Collections.emptySet());

        when(mockConfigClient.getConfiguredTables(eq(TEST_CONFIG_KEY))).thenReturn(configuredTables);

        assertThrows(RuntimeException.class, () -> underTest.getConfiguredTables(TEST_CONFIG_KEY));
    }
}