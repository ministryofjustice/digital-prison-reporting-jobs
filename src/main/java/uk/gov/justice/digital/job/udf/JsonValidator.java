package uk.gov.justice.digital.job.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.Optional;

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

        val originalData = objectMapper.readTree(originalJson);

        // Check that the original and parsed json trees match. If there are discrepancies then the initial parse by
        // from_json must have encountered an invalid field and set it to null (e.g. got string when int expected - this
        // will be set to null in the DataFrame.
        return originalData.equals(objectMapper.readTree(parsedJson)) &&
            allNotNullFieldsHaveValues(schema, originalData);
    }

    private boolean allNotNullFieldsHaveValues(StructType schema, JsonNode json) {
        for (StructField structField : schema.fields()) {
            // Skip fields that are declared nullable in the table schema.
            if (structField.nullable()) continue;

            val jsonField = Optional.ofNullable(json.get(structField.name()));

            val jsonFieldIsNull = jsonField
                .map(JsonNode::isNull)  // Field present in JSON but with no value
                .orElse(true);          // If no field found then it's null by default.

            // Verify that the current JSON field in the original data set is not null.
            if (jsonFieldIsNull) return false;
        }
        return true;
    }
}
