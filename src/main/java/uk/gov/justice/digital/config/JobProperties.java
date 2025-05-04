package uk.gov.justice.digital.config;

import javax.inject.Singleton;
import java.util.Map;
import java.util.Optional;

@Singleton
public class JobProperties {

    public static final String SPARK_JOB_NAME_PROPERTY = "spark.glue.JOB_NAME";
    public static final String SPARK_DRIVER_MEMORY_PROPERTY = "spark.driver.memory";
    public static final String SPARK_EXECUTOR_MEMORY_PROPERTY = "spark.executor.memory";

    public String getSparkJobName() {
        return getString(SPARK_JOB_NAME_PROPERTY);
    }

    public String getSparkDriverMemory() {
        return getString(SPARK_DRIVER_MEMORY_PROPERTY);
    }

    public String getSparkExecutorMemory() {
        return getString(SPARK_EXECUTOR_MEMORY_PROPERTY);
    }

    private static String getString(String propertyKey) {
        return Optional
                .ofNullable(System.getProperty(propertyKey))
                .orElseThrow(() -> new IllegalStateException("Property " + propertyKey + " not set"));
    }

    public JobProperties(Map<String, String> config) {
        config.forEach(System::setProperty);
    }

    public JobProperties() {}

}
