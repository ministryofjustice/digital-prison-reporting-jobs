package uk.gov.justice.digital.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PropertiesTest {

    private static final String SPARK_JOB_NAME_KEY = "spark.glue.JOB_NAME";
    private static final String SPARK_JOB_NAME = "SomeTestJob";

    @BeforeEach
    public void setupProperties() {
        System.setProperty(SPARK_JOB_NAME_KEY, SPARK_JOB_NAME);
    }

    @AfterEach
    public void cleanupProperties() {
        System.clearProperty(SPARK_JOB_NAME_KEY);
    }

    @Test
    public void shouldReturnJobNameWhenPropertySet() {
       assertEquals(SPARK_JOB_NAME, Properties.getSparkJobName());
    }

    @Test
    public void shouldThrowExceptionWhenJobNamePropertyNotSet() {
        System.clearProperty(SPARK_JOB_NAME_KEY);
        assertThrows(IllegalStateException.class, Properties::getSparkJobName);
    }
}