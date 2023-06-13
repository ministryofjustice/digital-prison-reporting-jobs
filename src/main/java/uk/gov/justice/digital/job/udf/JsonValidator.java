package uk.gov.justice.digital.job.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import lombok.val;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.job.filter.NomisDataFilter;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.udf;

/**
 * Simple JSON validator that is intended to be invoked *after* from_json is used to parse a field containing a JSON
 * string.
 * <p>
 * By design, from_json will forcibly enable nullable on all fields declared in the schema. This resolves potential
 * downstream issues with parquet but makes it harder for us to validate JSON principally because from_json
 *   o silently allows fields declared notNull to be null
 *   o silently converts fields with incompatible values to null
 * <p>
 * For this reason this validator *must* be used *after* first parsing a JSON string with from_json.
 * <p>
 * This validator performs the following checks
 *   o original and parsed json *must* be equal - if there is a difference this indicates a bad value in the source data
 *     e.g. a String value when a Numeric value was expected
 *   o all fields declared notNull *must* have a value
 *   o handling of dates represented in the incoming raw data as an ISO 8601 datetime string with the time values all
 *     set to zero
 * <p>
 * and can be used as follows within Spark SQL
 * <p>
 * StructType schema = .... // Some schema defining the format of the JSON being processed
 * <p>
 * UserDefinedFunction jsonValidator = JsonValidator.createAndRegister(
 *      schema,
 *      someDataFrame.sparkSession(),
 *      sourceName,
 *      tableName
 * );
 * <p>
 * // dataframe with a single string column 'rawJson' containing JSON to parse and validate against a schema
 * someDataFrame
 *  .withColumn("parsedJson", from_json(col("rawJson"), schema))
 *  .withColumn("valid", jsonValidator.apply(col("rawData"), to_json("parsedJson")))
 * <p>
 * The dataframe can then be filtered on the value of the boolean valid column where
 *  o true -> the JSON has passed validation
 *  o false -> the JSON has not passed validation
 */
public class JsonValidator implements Serializable {

    private static final long serialVersionUID = 3626262733334508950L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Ensure we render raw timestamps in the same format as Spark when comparing data during validation.
    private static final DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]");

    private static final Logger logger = LoggerFactory.getLogger(JsonValidator.class);

    public static UserDefinedFunction createAndRegister(
        StructType schema,
        SparkSession sparkSession,
        String schemaName,
        String tableName) {

        val jsonValidator = new JsonValidator();

        return sparkSession
            .udf()
            .register(
                String.format("udfValidatorFor%s%s", schemaName, tableName),
                udf(
                    (UDF2<String, String, Boolean>) (String originalJson, String parsedJson) ->
                        jsonValidator.validate(originalJson, parsedJson, schema),
                        DataTypes.BooleanType
                )
            );
    }

    public boolean validate(
        String originalJson,
        String parsedJson,
        StructType schema
    ) throws JsonProcessingException {

        // null content is still valid
        if(originalJson == null || parsedJson == null) return true;

        TypeReference<Map<String,Object>> mapTypeReference = new TypeReference<Map<String,Object>>() {};

        val originalData = objectMapper.readValue(originalJson, mapTypeReference);
        val parsedData = objectMapper.readValue(parsedJson, mapTypeReference);

        val nomisFilter = new NomisDataFilter(schema);

        // Apply filters
        val filteredData = nomisFilter.apply(originalData);

        // Reformat dates where appropriate so that the equality check passes for those cases where we can discard the
        // time part of any dates represented as datetimes in the raw data.
        // Also reduce precision of timestamps to milliseconds to avoid comparision failures.
//        val originalDataWithReformattedTemporalFields =
//                reformatTimestampFields(
//                        reformatDateFields(originalData, schema),
//                        schema
//                );


        // Check that
        //  o the original and parsed data match using a simple equality check
        //  o any fields declared not-nullable have a value
//        val result = originalDataWithReformattedTemporalFields.equals(parsedData) &&
//                allNotNullFieldsHaveValues(schema, objectMapper.readTree(originalJson));

        val result = filteredData.equals(parsedData) &&
                allNotNullFieldsHaveValues(schema, objectMapper.readTree(originalJson));

        logger.info("JSON validation result - json valid: {}", result);

        if (!result) {
            val difference = Maps.difference(filteredData, parsedData);
            if (difference.entriesDiffering().isEmpty()) {
                logger.error("JSON validation failed. At least one not-null field has no value");

            }
            else {
                logger.error("JSON validation failed. Parsed and Raw JSON have the following differences: {}",
                        difference.entriesDiffering()
                );
            }
        }

        return result;
    }

    private boolean allNotNullFieldsHaveValues(StructType schema, JsonNode json) {
        for (StructField structField : schema.fields()) {
            // Skip fields that are declared nullable in the table schema.
            if (structField.nullable()) continue;

            val jsonField = Optional.ofNullable(json.get(structField.name()));

            val jsonFieldIsNull = jsonField
                    .map(JsonNode::isNull)  // Field present in JSON but with no value
                    .orElse(true);          // If no field found then it's null by default.

            // We fail immediately if the field is null since it's declared as not-nullable.
            if (jsonFieldIsNull) {
                logger.error("JSON validation failed. Not null field {} is null", structField);
                return false;
            }
        }
        return true;
    }

    private Map<String, Object> reformatTimestampFields(Map<String, Object> data, StructType schema) {
        val timeStampFields = Arrays.stream(schema.fields())
                .filter(f -> f.dataType() == DataTypes.TimestampType)
                .map(StructField::name)
                .collect(Collectors.toSet());

        val updatedData = new HashMap<String, Object>();

        data.entrySet().forEach(entry -> {
            val updatedEntry = updateTimestampValue(timeStampFields, entry);
            updatedData.put(updatedEntry.getKey(), updatedEntry.getValue());
        });

        return updatedData;
    }

    private Map.Entry<String, Object> updateTimestampValue(Set<String> fields, Map.Entry<String, Object> entry) {
        return (fields.contains(entry.getKey()))
            ? Optional.ofNullable(entry.getValue())
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .flatMap(this::parseTimestamp)
                .map(d -> createEntry(entry, d))
                .orElse(entry)
                : entry;

    }

    private Optional<String> parseTimestamp(String ts) {
        try {
            val parsed = ZonedDateTime.parse(ts).truncatedTo(ChronoUnit.MILLIS);
            return Optional.of(timestampFormatter.format(parsed));
        }
        catch (Exception e) {
            return Optional.empty();
        }
    }

    private Map<String, Object> reformatDateFields(Map<String, Object> data, StructType schema) {
        val dateFields = Arrays.stream(schema.fields())
                .filter(f -> f.dataType() == DataTypes.DateType)
                .map(StructField::name)
                .collect(Collectors.toSet());

        // Collectors.toMap makes use of Hashmap.merge which expects the value to be not-null, which causes an NPE if
        // we have raw data with a null value. The non-streams based approach below works since we're putting values
        // directly instead.
        val updatedData = new HashMap<String, Object>();

        data.entrySet().forEach(entry -> {
            val updatedEntry = updateDateValue(dateFields, entry);
            updatedData.put(updatedEntry.getKey(), updatedEntry.getValue());
        });

        return updatedData;
    }

    private Map.Entry<String, Object> updateDateValue(Set<String> dateFields, Map.Entry<String, Object> entry) {
        // Only attempt to reformat those fields that have the Date type in our schema.
        return (dateFields.contains(entry.getKey()))
            ? Optional.ofNullable(entry.getValue())
                    .filter(String.class::isInstance)
                    .map(String.class::cast)
                    .map(rawDate -> parseAndValidateDate(entry.getKey(), rawDate))
                    .map(d -> createEntry(entry, d))
                    .orElse(entry)
            : entry;
    }

    private Map.Entry<String, Object> createEntry(Map.Entry<String, Object> entry, String d) {
        return new AbstractMap.SimpleEntry<>(entry.getKey(), d);
    }

    private String parseAndValidateDate(String fieldName, String rawDate) {
        // The raw date value may be represented as an ISO date time with a zeroed time part eg. 2012-01-01T00:00:00Z
        // Split the string on T so we get the date before the T, and the time after the T.
        // If we get two values we assume we're parsing a date and time, otherwise treat the value as a date.
        val dateParts = Arrays.asList(rawDate.split("T"));
        if (dateParts.size() == 2) {
            val dateString = dateParts.get(0);
            val timeString = dateParts.get(1);
            if (timeString != null && (timeString.replaceAll("[^1-9]", "").compareTo("0") > 0))
                logger.warn("Discarding populated timestamp {} when converting {} to a date", timeString, fieldName);
            // If the time part contains values other than zero log a warning since we could be losing data here by
            // discarding the time part during the conversion to date.
            return dateString;
        }
        else return rawDate;
    }

}
