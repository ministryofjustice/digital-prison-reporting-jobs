package uk.gov.justice.digital.config;

import java.util.Optional;

public class Properties {

    private static final String SPARK_JOB_NAME_PROPERTY = "spark.glue.JOB_NAME";

    public static String getSparkJobName() {
        return Optional
            .ofNullable(System.getProperty(SPARK_JOB_NAME_PROPERTY))
            .orElseThrow(() -> new IllegalStateException("Property " + SPARK_JOB_NAME_PROPERTY + " not set"));
    }


}
