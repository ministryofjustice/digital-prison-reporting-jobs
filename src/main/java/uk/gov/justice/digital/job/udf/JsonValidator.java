package uk.gov.justice.digital.job.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Bean;
import lombok.val;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

import static org.apache.spark.sql.functions.udf;

public class JsonValidator {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final UserDefinedFunction registeredFunction;

    public JsonValidator(StructType schema, SparkSession sparkSession, String schemaName, String tableName) {
        val udfName = String.format("udfValidatorFor%s%s", schemaName, tableName);
        this.registeredFunction = sparkSession
            .udf()
            .register(udfName, jsonValidatorForSchema(schema));
    }

    public UserDefinedFunction getRegisteredFunction() {
        return registeredFunction;
    }

    private UserDefinedFunction jsonValidatorForSchema(StructType schema) {
        return udf(
            (UDF2<String, String, Boolean>) (String originalJson, String parsedJson) ->
                validateJson(originalJson, parsedJson, schema),
            DataTypes.BooleanType
        );
    }

    private boolean validateJson(
        String originalJson,
        String parsedJson,
        StructType schema
    ) throws JsonProcessingException {

        val originalData = objectMapper.readTree(originalJson);
        val parsedData = objectMapper.readTree(parsedJson);

        // Check that the original and parsed json trees match. If there are discrepancies then the initial parse by
        // from_json must have encountered an invalid field and set it to null (e.g. got string when int expected - this
        // will be set to null in the DataFrame.
        if (!originalData.equals(parsedData)) return false;


        // Verify that all notNull fields have a value set.
        for (StructField structField : schema.fields()) {
            // Skip fields that are declared nullable in the table schema.
            if (structField.nullable()) continue;

            val jsonField = Optional.ofNullable(originalData.get(structField.name()));

            val jsonFieldIsNull = jsonField
                .map(JsonNode::isNull)  // Field present in JSON but with no value
                .orElse(true);         // If no field found then it's null by default.

            // Verify that the current JSON field in the original data set is not null.
            if (jsonFieldIsNull) return false;
        }

        return true;
    }
}
