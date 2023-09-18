package uk.gov.justice.digital.config;

import javax.inject.Singleton;
import java.io.Serializable;
import java.util.Optional;

@Singleton
public class JobPropertiesImpl implements JobProperties, Serializable {

    private static final long serialVersionUID = 3308505483047615332L;

    private static final String SPARK_JOB_NAME_PROPERTY = "spark.glue.JOB_NAME";

    public String getSparkJobName() {
        return Optional
            .ofNullable(System.getProperty(SPARK_JOB_NAME_PROPERTY))
            .orElseThrow(() -> new IllegalStateException("Property " + SPARK_JOB_NAME_PROPERTY + " not set"));
    }


}
