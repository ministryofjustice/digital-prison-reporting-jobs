package uk.gov.justice.digital.job.filter.field;

import lombok.val;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Optional;

// Filter that converts Timestamp data in a field declared as a DateType into a date.
public class TimestampToDateFieldFilter extends FieldFilter {

    private static final Logger logger = LoggerFactory.getLogger(TimestampToDateFieldFilter.class);

    public TimestampToDateFieldFilter(StructType schema) {
        super(getApplicableFieldsFromSchema(schema, e -> e.dataType() == DataTypes.DateType));
    }

    @Override
    protected Entry<String, Object> applyFilterToEntry(Entry<String, Object> entry) {
        val value = Optional.ofNullable(entry.getValue())
                .map(Object::toString)
                .orElse("");

        // The raw date value may be represented as an ISO date time with a zeroed time part eg. 2012-01-01T00:00:00Z
        // Split the string on T so we get the date before the T, and the time after the T.
        // If we get two values we assume we're parsing a date and time, otherwise assume we assume it's a date.
        val dateParts = Arrays.asList(value.split("T"));

        if (dateParts.size() == 2) {
            val dateString = dateParts.get(0);
            val timeString = dateParts.get(1);
            if (timeStringIsPopulated(timeString))
                logger.warn("Discarding populated timestamp {} when converting {} to a date", timeString, entry.getKey());
            // If the time part contains values other than zero log a warning since we could be losing data here by
            // discarding the time part during the conversion to date.
            entry.setValue(dateString);
        }
        return entry;
    }

    private boolean timeStringIsPopulated(String ts) {
        // Removing anything that isn't a number in the range 1 to 9 from the string and then compare the result to 0.
        // Any result greater than zero indicates that at least one digit in the time value is set to a non-zero digit.
        return (ts.replaceAll("[^1-9]", "").compareTo("0") > 0);
    }
}
