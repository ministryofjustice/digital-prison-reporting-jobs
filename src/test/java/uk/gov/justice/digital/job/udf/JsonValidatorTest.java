package uk.gov.justice.digital.job.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.*;

import static java.util.AbstractMap.SimpleEntry;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

class JsonValidatorTest {

    private static final class Fields {
        public static final String MANDATORY = "mandatory";
        public static final String OPTIONAL = "optional";
        public static final String NUMERIC = "numeric";
    }

    private static final StructType schema =
            new StructType()
                    .add(Fields.MANDATORY, StringType, false)
                    .add(Fields.OPTIONAL, StringType, true)
                    .add(Fields.NUMERIC, IntegerType, true);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void shouldPassValidJsonWithOnlyMandatoryFieldSet() throws JsonProcessingException {
        val json = createJsonFromEntries(Collections.singletonList(entry(Fields.MANDATORY, "somevalue")));
        assertThat(JsonValidator.validate(json, json, schema), is(emptyString()));
    }

    @Test
    public void shouldPassValidJsonWithAllFieldsSet() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "somevalue"),
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, 1)
        ));
        assertThat(JsonValidator.validate(json, json, schema), is(emptyString()));
    }

    @Test
    public void shouldPassValidJsonIrrespectiveOfFieldOrdering() throws JsonProcessingException {
        val json = "{\"mandatory\":\"foo\",\"numeric\":1}";
        val jsonWithReverseFieldOrder = "{\"numeric\":1,\"mandatory\":\"foo\"}";
        assertThat(JsonValidator.validate(json, jsonWithReverseFieldOrder, schema), is(emptyString()));
    }

    @Test
    public void shouldFailJsonWithMissingMandatoryValue() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, 1)
        ));
        assertThat(JsonValidator.validate(json, json, schema), not(emptyString()));
    }

    @Test
    public void shouldFailWhenRawAndParsedJsonDataDiffers() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "somevalue"),
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, "12")
        ));

        val fakeParsedJson = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "additonalValue"),
                entry(Fields.OPTIONAL, "somethingElse"),
                entry(Fields.NUMERIC, "42")
        ));

        assertThat(JsonValidator.validate(json, fakeParsedJson, schema), not(emptyString()));
    }

    /**
     * This test exercises the specific case where we have used from_json to parse a JSON string against a schema.
     * If a value doesn't match the type specified for the field the value is returned as NULL. For this reason we
     * must compare the original and parsed JSON data since any differences will be down to invalid types.
     */
    @Test
    public void shouldFailWhenParsedJsonContainsNullWhenValuePresentInRawData() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "somevalue"),
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, "this is not a number")
        ));

        // Here we replicate the invalid type handling by leaving the NUMERIC field unset.
        val fakeParsedJson = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "somevalue"),
                entry(Fields.OPTIONAL, "anotherValue")
        ));

        assertThat(JsonValidator.validate(json, fakeParsedJson, schema), not(emptyString()));
    }

    @Test
    public void shouldFailWhenComparingJsonStringAndNullObject() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "somevalue"),
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, "this is not a number")
        ));

        assertThat(JsonValidator.validate(null, json, schema), not(emptyString()));
        assertThat(JsonValidator.validate(json, null, schema), not(emptyString()));
    }

    @Test
    public void shouldTreatMissingFieldsAsEqualToNullFields() throws JsonProcessingException {
        val optionalFieldIsNull = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "someValue"),
                entry(Fields.OPTIONAL, null),
                entry(Fields.NUMERIC, "42")
        ));

        val optionalFieldMissing = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "someValue"),
                entry(Fields.NUMERIC, "42")
        ));

        assertThat(
                JsonValidator.validate(optionalFieldMissing, optionalFieldIsNull, schema),
                is(emptyString())
        );

        assertThat(
                JsonValidator.validate(optionalFieldIsNull, optionalFieldMissing, schema),
                is(emptyString())
        );
    }

    @Test
    public void shouldTreatNonNullOptionalFieldsAsDifferentFromMissingFields() throws JsonProcessingException {
        val optionalFieldNotNull = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "someValue"),
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, "42")
        ));

        val optionalFieldMissing = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "someValue"),
                entry(Fields.NUMERIC, "42")
        ));

        assertThat(
                JsonValidator.validate(optionalFieldMissing, optionalFieldNotNull, schema),
                not(emptyString())
        );

        assertThat(
                JsonValidator.validate(optionalFieldNotNull, optionalFieldMissing, schema),
                not(emptyString())
        );
    }

    private static SimpleEntry<String, Object> entry(String key, Object value) {
        return new SimpleEntry<>(key, value);
    }

    private static String createJsonFromEntries(List<SimpleEntry<String, Object>> entries) throws JsonProcessingException {
        val map = new HashMap<>();
        entries.forEach(answer -> map.put(answer.getKey(), answer.getValue()));
        return objectMapper.writeValueAsString(map);
    }

}