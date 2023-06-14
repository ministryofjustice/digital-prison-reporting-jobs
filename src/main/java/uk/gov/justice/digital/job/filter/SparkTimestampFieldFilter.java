package uk.gov.justice.digital.job.filter;

import lombok.val;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map.Entry;

// Filter that re-formats raw timestamps into the standard Spark format.
public class SparkTimestampFieldFilter extends FieldFilter {

    // Ensure we render raw timestamps in the same format as Spark when comparing data during validation.
    private static final DateTimeFormatter timestampFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]");

    private static final Logger logger = LoggerFactory.getLogger(SparkTimestampFieldFilter.class);

    public SparkTimestampFieldFilter(StructType schema) {
        super(getApplicableFieldsFromSchema(schema, e -> e.dataType() == DataTypes.TimestampType));
    }

    @Override
    public Entry<String, Object> applyFilterToEntry(Entry<String, Object> entry) {
        try {
            val parsed = ZonedDateTime.parse(entry.getValue().toString()).truncatedTo(ChronoUnit.MILLIS);
            entry.setValue(timestampFormatter.format(parsed));
            return entry;
        }
        catch (Exception e) {
            logger.warn("Caught exception when parsing field: {} timestamp: {}", entry.getKey(), entry.getValue(), e);
            return entry;
        }
    }

}
