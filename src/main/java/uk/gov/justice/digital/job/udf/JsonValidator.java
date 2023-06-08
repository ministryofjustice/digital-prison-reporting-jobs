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

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.udf;

/**
 * Simple JSON validator that is intended to be invoked *after* from_json is used to parse a field containing a JSON
 * string.
 *
 * By design, from_json will forcibly enable nullable on all fields declared in the schema. This resolves potential
 * downstream issues with parquet but makes it harder for us to validate JSON principally because from_json
 *   o silently allows fields declared notNull to be null
 *   o silently converts fields with incompatible values to null
 *
 * For this reason this validator *must* be used *after* first parsing a JSON string with from_json.
 *
 * This validator performs the following checks
 *   o original and parsed json *must* be equal - if there is a difference this indicates a bad value in the source data
 *     e.g. a String value when a Numeric value was expected
 *   o all fields declared notNull *must* have a value
 *   o handling of dates represented in the incoming raw data as an ISO 8601 datetime string with the time values all
 *     set to zero
 *
 * and can be used as follows within Spark SQL
 *
 * StructType schema = .... // Some schema defining the format of the JSON being processed
 *
 * UserDefinedFunction jsonValidator = JsonValidator.createAndRegister(
 *      schema,
 *      someDataFrame.sparkSession(),
 *      sourceName,
 *      tableName
 * );
 *
 * // dataframe with a single string column 'rawJson' containing JSON to parse and validate against a schema
 * someDataFrame
 *  .withColumn("parsedJson", from_json(col("rawJson"), schema))
 *  .withColumn("valid", jsonValidator.apply(col("rawData"), to_json("parsedJson")))
 *
 * The dataframe can then be filtered on the value of the boolean valid column where
 *  o true -> the JSON has passed validation
 *  o false -> the JSON has not passed validation
 */
public class JsonValidator implements Serializable {

    private static final long serialVersionUID = 3626262733334508950L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

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
        val parsedData = objectMapper.readTree(parsedJson);

        val originalJsonWithFormattedDates = objectMapper.writeValueAsString(reformatDateFields(originalData, schema));
        val originalDataWithFormattedDates = objectMapper.readTree(originalJsonWithFormattedDates);

        // Check that the original and parsed json trees match. If there are discrepancies then the initial parse by
        // from_json must have encountered an invalid field and set it to null (e.g. got string when int expected - this
        // will be set to null in the DataFrame. Also allow through any dates represented as datetimes with a zeroed
        // time part and ensure that all fields declared not-null have a value.
        return originalDataWithFormattedDates.equals(parsedData) &&
                allNotNullFieldsHaveValues(schema, objectMapper.readTree(originalJson));
    }

    private boolean allNotNullFieldsHaveValues(StructType schema, JsonNode json) {
        for (StructField structField : schema.fields()) {
            // Skip fields that are declared nullable in the table schema.
            if (structField.nullable()) continue;

            val jsonField = Optional.ofNullable(json.get(structField.name()));

            val jsonFieldIsNull = jsonField
                    .map(JsonNode::isNull)  // Field present in JSON but with no value
                    .orElse(true);          // If no field found then it's null by default.

            // We fail immediately if the field is null since it should have a value because it's declared as not-null.
            if (jsonFieldIsNull) return false;
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
    private boolean timeIsUnset(String time) {
        val numbersOnly = time.replaceAll("[^0-9]", "");
        return Integer.parseInt(numbersOnly) > 0;
    }

}
