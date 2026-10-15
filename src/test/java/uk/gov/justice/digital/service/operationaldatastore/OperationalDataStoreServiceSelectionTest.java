package uk.gov.justice.digital.service.operationaldatastore;

import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.BeanDefinition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.config.JobArguments.OPERATIONAL_DATA_STORE_WRITE_ENABLED;

class OperationalDataStoreServiceSelectionTest {

    @Test
    void shouldUseTheRealServiceWhenWriteIsEnabled() {
        assertEquals(OperationalDataStoreServiceImpl.class, selectedServiceFor(Map.of(OPERATIONAL_DATA_STORE_WRITE_ENABLED, "true")));
    }

    @ParameterizedTest
    @CsvSource({ "false", "False", "no", "''" })
    void shouldUseTheDisabledServiceWhenWriteIsNotEnabled(String value) {
        assertEquals(DisabledOperationalDataStoreService.class, selectedServiceFor(Map.of(OPERATIONAL_DATA_STORE_WRITE_ENABLED, value)));
    }

    @Test
    void shouldUseTheDisabledServiceWhenWriteEnabledIsMissing() {
        assertEquals(DisabledOperationalDataStoreService.class, selectedServiceFor(Collections.emptyMap()));
    }

    // Only looks at bean definitions, so neither service is instantiated.
    private static Class<?> selectedServiceFor(Map<String, Object> properties) {
        try (ApplicationContext context = ApplicationContext.run(properties)) {
            Collection<BeanDefinition<OperationalDataStoreService>> definitions =
                    context.getBeanDefinitions(OperationalDataStoreService.class);
            assertEquals(1, definitions.size());
            return definitions.iterator().next().getBeanType();
        }
    }
}
