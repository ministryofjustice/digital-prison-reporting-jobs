package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import uk.gov.justice.digital.client.s3.S3ConfigReaderClient;
import uk.gov.justice.digital.exception.ConfigServiceException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ConfigService {

    private final S3ConfigReaderClient configClient;

    @Inject
    public ConfigService(S3ConfigReaderClient configClient) {
        this.configClient = configClient;
    }

    public ImmutableSet<ImmutablePair<String, String>> getConfiguredTables(String configKey) {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configClient.getConfiguredTables(configKey);
        if (configuredTables.isEmpty()) throw new ConfigServiceException("No tables configured for key " + configKey);
        return configuredTables;
    }

    public ImmutableSet<String> getConfiguredTablePaths(String configKey) {
        return ImmutableSet.copyOf(getConfiguredTables(configKey)
                .stream()
                .map(pair -> pair.left + "/" + pair.right)
                .iterator()
        );
    }
}
