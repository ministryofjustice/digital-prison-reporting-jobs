package uk.gov.justice.digital.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.gov.justice.digital.config.JobProperties.*;

class JobPropertiesTest {

    private static final String SPARK_JOB_NAME = "SomeTestJob";
    private static final String DRIVER_MEMORY = "1g";
    private static final String EXECUTOR_MEMORY = "2g";

    private static final JobProperties underTest = new JobProperties();

    @BeforeEach
    public void setupProperties() {
        System.setProperty(SPARK_JOB_NAME_PROPERTY, SPARK_JOB_NAME);
        System.setProperty(SPARK_DRIVER_MEMORY_PROPERTY, DRIVER_MEMORY);
        System.setProperty(SPARK_EXECUTOR_MEMORY_PROPERTY, EXECUTOR_MEMORY);
    }

    @AfterEach
    public void cleanupProperties() {
        System.clearProperty(SPARK_JOB_NAME_PROPERTY);
        System.clearProperty(SPARK_DRIVER_MEMORY_PROPERTY);
        System.clearProperty(SPARK_EXECUTOR_MEMORY_PROPERTY);
    }

    @Test
    void shouldReturnJobNameWhenPropertySet() {
        assertEquals(SPARK_JOB_NAME, underTest.getSparkJobName());
    }

    @Test
    void shouldThrowExceptionWhenJobNamePropertyNotSet() {
        System.clearProperty(SPARK_JOB_NAME_PROPERTY);
        assertThrows(IllegalStateException.class, underTest::getSparkJobName);
    }

    @Test
    void shouldReturnDriverMemoryWhenPropertyIsSet() {
        assertEquals(DRIVER_MEMORY, underTest.getSparkDriverMemory());
    }

    @Test
    void shouldThrowExceptionWhenDriverMemoryPropertyIsNotSet() {
        System.clearProperty(SPARK_DRIVER_MEMORY_PROPERTY);
        assertThrows(IllegalStateException.class, underTest::getSparkDriverMemory);
    }

    @Test
    void shouldReturnExecutorMemoryWhenPropertyIsSet() {
        assertEquals(EXECUTOR_MEMORY, underTest.getSparkExecutorMemory());
    }

    @Test
    void shouldThrowExceptionWhenExecutorMemoryPropertyIsNotSet() {
        System.clearProperty(SPARK_EXECUTOR_MEMORY_PROPERTY);
        assertThrows(IllegalStateException.class, underTest::getSparkExecutorMemory);
    }
}