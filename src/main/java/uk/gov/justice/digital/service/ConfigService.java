package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import uk.gov.justice.digital.client.s3.S3ConfigReaderClient;

import javax.inject.Singleton;
import java.util.Set;

@Singleton
public class ConfigService {

    private final S3ConfigReaderClient configClient;

    public ConfigService(S3ConfigReaderClient configClient) {
        this.configClient = configClient;
    }

    public ImmutableSet<ImmutablePair<String, String>> getConfiguredTables(String configKey) {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configClient.getConfiguredTables(configKey);
        if (configuredTables.isEmpty()) throw new RuntimeException("No tables configured for key " + configKey);
        return configuredTables;
    }
}
