package uk.gov.justice.digital.job.filter;

import lombok.val;
import org.apache.avro.util.MapEntry;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.jupiter.api.Assertions.*;

class TimestampToDateFieldFilterTest {

    public static final String DATE = "date";
    public static final String OTHER = "other";

    private static final StructType schemaWithDate =
            new StructType()
                    .add(DATE, DateType, false)
                    .add(OTHER, StringType, false);

    private static final TimestampToDateFieldFilter underTest = new TimestampToDateFieldFilter(schemaWithDate);

    @Test
    public void isEligibleShouldReturnTrueForTimestampField() {
        assertTrue(underTest.isEligible(DATE));
    }

    @Test
    public void isEligibleShouldReturnFalseForAnyOtherField() {
        assertFalse(underTest.isEligible(OTHER));
    }

    @Test
    public void shouldReturnValueWhereRawDataContainsADateOnly() {
        val result = underTest.apply(new MapEntry<>(DATE, "2006-01-01"));
        assertEquals("2006-01-01", result.getValue());
    }

    @Test
    public void shouldReturnDateValueWhereRawDataContainsADateTime() {
        val result = underTest.apply(new MapEntry<>(DATE, "2006-01-01T00:00:00.000Z"));
        assertEquals("2006-01-01", result.getValue());
    }

    @Test
    public void shouldReturnDateValueWhereRawDataContainsADateTimeWithTimeValuesSet() {
        val result = underTest.apply(new MapEntry<>(DATE, "2006-01-01T12:34:56.789Z"));
        assertEquals("2006-01-01", result.getValue());
    }

    @Test
    public void shouldReturnOriginalValueWhereRawDataCannotBeParsed() {
        val result = underTest.apply(new MapEntry<>(DATE, "foo"));
        assertEquals("foo", result.getValue());
    }

    @Test
    public void shouldHandleNullValue() {
        val result = underTest.apply(new MapEntry<>(DATE, null));
        assertNull(result.getValue());
    }

}