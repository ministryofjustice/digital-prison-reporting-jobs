package uk.gov.justice.digital.job.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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

        // Reformat dates where appropriate so that the equality check passes for those cases where we can discard the
        // time part of any dates represented as datetimes in the raw data.
        val originalDataWithReformattedDates = reformatDateFields(originalData, schema);

        // Check that
        //  o the original and parsed data match using a simple equality check
        //  o any fields declared not-nullable have a value
        val result = originalData.equals(objectMapper.readTree(parsedJson)) &&
            allNotNullFieldsHaveValues(schema, originalData);

        logger.info("JSON validation result - json valid: {}", result);

        if (!result) {
            logger.error("JSON validation failed. Parsed and Raw JSON differs. Parsed: '{}', Raw: '{}'",
                    parsedJson,
                    originalJson
            );
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

    private Map<String, Object> reformatDateFields(Map<String, Object> data, StructType schema) {
        val dateFields = Arrays.stream(schema.fields())
                .filter(f -> f.dataType() == DataTypes.DateType)
                .map(StructField::name)
                .collect(Collectors.toSet());

        return data.entrySet().stream()
                .map(entry -> updateDateValue(dateFields, entry))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map.Entry<String, Object> updateDateValue(Set<String> dateFields, Map.Entry<String, Object> entry) {
        // Only attempt to reformat those fields that have the Date type in our schema.
        return (dateFields.contains(entry.getKey()))
            ? Optional.of(entry.getValue())
                    .filter(String.class::isInstance)
                    .map(String.class::cast)
                    .map(this::parseAndValidateDate)
                    .map(d -> createEntry(entry, d))
                    .orElse(entry)
            : entry;
    }

    private Map.Entry<String, Object> createEntry(Map.Entry<String, Object> entry, String d) {
        return new AbstractMap.SimpleEntry<>(entry.getKey(), d);
    }

    private String parseAndValidateDate(String date) {
        // The raw date value may be represented as an ISO date time with a zeroed time part eg. 2012-01-01T00:00:00Z
        // Split the string on T so we get the date before the T, and the time after the T.
        // If we get two values we assume we're parsing a date and time, otherwise treat the value as a date.
        val dateParts = Arrays.asList(date.split("T"));
        if (dateParts.size() == 2) {
            val dateString = dateParts.get(0);
            val timeString = dateParts.get(1);
            return (timeIsUnset(timeString))
                ? date
                : dateString;
        }
        else return date;
    }

    // We assume the time is unset if the sum of all the numbers in the time string is zero. Anything greater than
    // zero implies that at least one of the values is set in the time string in which case, this will fail validation.
    // If there are no numbers in the string we return false to trigger a validation failure.
    private boolean timeIsUnset(String time) {
        val numbersOnly = time.replaceAll("[^0-9]", "");
        return numbersOnly.length() > 0 && Integer.parseInt(numbersOnly) > 0;
    }

}
