package uk.gov.justice.digital.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import uk.gov.justice.digital.client.dms.DmsClient;
import uk.gov.justice.digital.client.s3.S3ConfigReaderClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.ConfigServiceException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ConfigService {

    private final JobArguments arguments;
    private final DmsClient dmsClient;
    private final S3ConfigReaderClient s3ConfigClient;

    @Inject
    public ConfigService(JobArguments arguments, DmsClient dmsClient, S3ConfigReaderClient s3ConfigClient) {
        this.arguments = arguments;
        this.dmsClient = dmsClient;
        this.s3ConfigClient = s3ConfigClient;
    }

    public ImmutableSet<ImmutablePair<String, String>> getConfiguredTables(String configKey) {
        ImmutableSet<ImmutablePair<String, String>> configuredTables;

        if (arguments.readConfigFromS3()) {
            configuredTables = s3ConfigClient.getConfiguredTables(configKey);
        } else {
            try {
                configuredTables = dmsClient.getReplicationTaskTables(configKey);
            } catch (JsonProcessingException e) {
                throw new ConfigServiceException("Failed to get ingested tables from replication task for domain " + configKey);
            }
        }

        if (configuredTables.isEmpty()) {
            throw new ConfigServiceException("No tables configured for key " + configKey);
        }

        return configuredTables;
    }
}
