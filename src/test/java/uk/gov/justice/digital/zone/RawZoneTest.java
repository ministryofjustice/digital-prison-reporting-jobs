package uk.gov.justice.digital.zone;

import lombok.val;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.JobParameters;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;

public class RawZoneTest {

    private static final String S3_PATH_KEY = "dpr.raw.s3.path";
    private static final String S3_PATH = "s3://loadjob/raw";

    private final JobParameters jobParameters = new JobParameters(Collections.singletonMap(S3_PATH_KEY, S3_PATH));

    private final RawZone underTest = new RawZone(jobParameters);

    @Test
    public void shouldReturnValidRawS3Path() {
        val source = "oms_owner";
        val table  = "agency_internal_locations";
        val operation = "load";

        val expectedRawS3Path = String.join("/", S3_PATH, source, table, operation);

        assertEquals(expectedRawS3Path, underTest.getTablePath(S3_PATH, source, table, operation));
    }
}
