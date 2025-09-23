package uk.gov.justice.digital.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import uk.gov.justice.digital.client.dms.DmsClient;
import uk.gov.justice.digital.exception.ConfigServiceException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ConfigService {

    private final DmsClient dmsClient;

    @Inject
    public ConfigService(DmsClient dmsClient) {
        this.dmsClient = dmsClient;
    }

    public ImmutableSet<ImmutablePair<String, String>> getConfiguredTables(String configKey) {
        try {
            ImmutableSet<ImmutablePair<String, String>> configuredTables = dmsClient.getReplicationTaskTables(configKey);
            if (configuredTables.isEmpty()) throw new ConfigServiceException("No tables configured for key " + configKey);
            return configuredTables;
        } catch (JsonProcessingException e) {
            throw new ConfigServiceException("Failed to get ingested tables from replication task for domain " + configKey);
        }
    }
}
