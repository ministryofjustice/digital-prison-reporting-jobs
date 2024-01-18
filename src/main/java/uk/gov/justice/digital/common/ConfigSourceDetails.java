package uk.gov.justice.digital.common;

public class ConfigSourceDetails {

    private final String bucket;
    private final String configKey;

    public ConfigSourceDetails(String bucket, String configKey) {
        this.bucket = bucket;
        this.configKey = configKey;
    }

    public String getBucket() {
        return bucket;
    }

    public String getConfigKey() {
        return configKey;
    }
}
