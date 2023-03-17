package uk.gov.justice.digital.zone;

import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.JobParameters;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

public class RawZoneTest {

    private static final Map<String, String> testConfig;
    private static final String S3_RAW_PATH_KEY = "raw.s3.path";
    private static final String S3_RAW_PATH = "s3://loadjob/raw";

    static {
        testConfig = new HashMap<>();
        testConfig.put(S3_RAW_PATH_KEY, S3_RAW_PATH);
    }

    private static final JobParameters jobParameters = new JobParameters(testConfig);
    private static final RawZone rawZone = new RawZone(jobParameters);

    @Test
    public void shouldReturnValidRawS3Prefix() {
        final String expectedPrefix = "s3://loadjob/raw";
        assertEquals(expectedPrefix, rawZone.getRawPath());
    }

    @Test
    public void shouldReturnValidRawS3Path() {
        final String prefix = "s3://loadjob/raw";
        final String source = "oms_owner";
        final String table  = "agency_internal_locations";
        final String operation = "load";

        final String expectedRawS3Path = "s3://loadjob/raw/oms_owner/agency_internal_locations/load";
        final String actualRawS3PAth = rawZone.getTablePath(prefix, source, table, operation);
        assertEquals(expectedRawS3Path, actualRawS3PAth);
    }
}
