package uk.gov.justice.digital.datahub.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class JDBCGlueConnectionDetailsTest {

    @Test
    void toSparkJdbcPropertiesCorrectlyMapsProperties() {
        JDBCGlueConnectionDetails underTest = new JDBCGlueConnectionDetails(
                "url",
                "org.postgresql.Driver",
                new JDBCCredentials("user", "password")
        );

        Properties properties = underTest.toSparkJdbcProperties();

        Set<String> expectedPropertyNames = new HashSet<>(Arrays.asList(
                "driver", "user", "password"
        ));
        assertEquals(expectedPropertyNames, properties.stringPropertyNames());

        assertEquals("org.postgresql.Driver", properties.getProperty("driver"));
        assertEquals("user", properties.getProperty("user"));
        assertEquals("password", properties.getProperty("password"));
    }

}