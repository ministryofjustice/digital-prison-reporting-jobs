package uk.gov.justice.digital.job.validator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import lombok.val;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.job.filter.NomisDataFilter;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * JSON validator that is intended to be invoked *after* from_json is used to parse a field containing a JSON string.
 * <p>>
 * Data is filtered prior to validation to ensure that the raw data matches the expected spark representation so that
 * validation passes.
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
 */
public class JsonValidator {

    public static class JsonValidatorStatic {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        private static final Logger logger = LoggerFactory.getLogger(JsonValidator.class);

        public static String validate(
                String originalJson,
                String parsedJson,
                StructType schema
        ) {

            // null content is still valid
            if (originalJson == null || parsedJson == null) return "Json data was parsed as null";

            TypeReference<Map<String,Object>> mapTypeReference = new TypeReference<Map<String,Object>>() {};

            try {
                val originalData = objectMapper.readValue(originalJson, mapTypeReference);
                val parsedDataWithNullColumnsDropped = removeNullValues(objectMapper.readValue(parsedJson, mapTypeReference));

                val nomisFilter = new NomisDataFilter(schema);

                // Apply data filters. See NomisDataFilter.
                val filteredDataWithNullColumnsDropped = removeNullValues(nomisFilter.apply(originalData));

                // Check that
                //  o the original and parsed data match using a simple equality check
                //  o any fields declared not-nullable have a value
                val result = filteredDataWithNullColumnsDropped.equals(parsedDataWithNullColumnsDropped) &&
                        allNotNullFieldsHaveValues(schema, objectMapper.readTree(originalJson));

                logger.debug("JSON validation result - json valid: {}", result);

                if (!result) {
                    // We treat null fields the same as missing fields
                    val difference = Maps.difference(filteredDataWithNullColumnsDropped, parsedDataWithNullColumnsDropped);

                    val errorMessage = String.format("JSON validation failed. Parsed and Raw JSON have the following differences: %s", difference);
                    logger.error(errorMessage);
                    return errorMessage;
                } else return "";
            } catch (JsonProcessingException e) {
                String errorMessage = "Failed to process record as json" + e.getMessage();
                logger.error(errorMessage, e);
                return errorMessage;
            }

        }

        private static boolean allNotNullFieldsHaveValues(StructType schema, JsonNode json) {
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

        private static Map<String, Object> removeNullValues(Map<String, Object> map) {
            return map
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() != null)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

}
