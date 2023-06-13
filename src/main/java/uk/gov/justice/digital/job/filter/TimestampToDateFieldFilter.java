package uk.gov.justice.digital.job.filter;

import lombok.val;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// Filter that converts Timestamp data in a field declared as a DateType into a date.
public class TimestampToDateFieldFilter implements FieldFilter {

    private static final Logger logger = LoggerFactory.getLogger(TimestampToDateFieldFilter.class);

    private final Set<String> eligibleFields;

    public TimestampToDateFieldFilter(StructType schema) {
        // This filter applies to Timestamp fields.
        eligibleFields = Arrays.stream(schema.fields())
                .filter(f -> f.dataType() == DataTypes.DateType)
                .map(StructField::name)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean isEligible(String fieldName) {
        return eligibleFields.contains(fieldName);
    }

    @Override
    public Entry<String, Object> apply(Entry<String, Object> entry) {
        val value = Optional.ofNullable(entry.getValue())
                .map(Object::toString)
                .orElse("");

        // The raw date value may be represented as an ISO date time with a zeroed time part eg. 2012-01-01T00:00:00Z
        // Split the string on T so we get the date before the T, and the time after the T.
        // If we get two values we assume we're parsing a date and time, otherwise treat the value as a date.
        val dateParts = Arrays.asList(value.split("T"));

        if (dateParts.size() == 2) {
            val dateString = dateParts.get(0);
            val timeString = dateParts.get(1);
            if (timeString != null && (timeString.replaceAll("[^1-9]", "").compareTo("0") > 0))
                logger.warn("Discarding populated timestamp {} when converting {} to a date", timeString, entry.getKey());
            // If the time part contains values other than zero log a warning since we could be losing data here by
            // discarding the time part during the conversion to date.
            entry.setValue(dateString);
        }
        return entry;
    }
}
