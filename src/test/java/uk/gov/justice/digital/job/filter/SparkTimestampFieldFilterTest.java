package uk.gov.justice.digital.job.filter;

import lombok.val;
import org.apache.avro.util.MapEntry;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.job.filter.field.SparkTimestampFieldFilter;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.junit.jupiter.api.Assertions.*;

class SparkTimestampFieldFilterTest {

    public static final String TIMESTAMP = "timestamp";
    public static final String OTHER = "other";

    private static final StructType schemaWithTimestamp =
            new StructType()
                    .add(TIMESTAMP, TimestampType, false)
                    .add(OTHER, StringType, false);

    private static final SparkTimestampFieldFilter underTest = new SparkTimestampFieldFilter(schemaWithTimestamp);

    @Test
    public void shouldOnlyModifyApplicableFields() {
        val result = underTest.apply(new MapEntry<>(OTHER, "2006-01-01T12:34:56.789Z"));
        assertEquals("2006-01-01T12:34:56.789Z", result.getValue());
    }

    @Test
    public void shouldReformatTimestampsWithPrecisionThatExceedsTheSparkFormat() {
        val result = underTest.apply(new MapEntry<>(TIMESTAMP, "2006-01-01T12:01:23.123456789Z"));
        assertEquals("2006-01-01T12:01:23.123Z", result.getValue());
    }

    @Test
    public void shouldReformatTimestampsContainingNoFractionalSeconds() {
        val result = underTest.apply(new MapEntry<>(TIMESTAMP, "2006-01-01T12:01:23Z"));
        assertEquals("2006-01-01T12:01:23.000Z", result.getValue());
    }

    @Test
    public void shouldReturnTheOriginalEntryIfParsingFails() {
        val result = underTest.apply(new MapEntry<>(TIMESTAMP, "foo"));
        assertEquals("foo", result.getValue());
    }

    @Test
    public void shouldHandleNullValue() {
        val result = underTest.apply(new MapEntry<>(TIMESTAMP, null));
        assertNull(result.getValue());
    }

}