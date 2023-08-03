package uk.gov.justice.digital.job.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.apache.logging.log4j.core.util.Assert;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.AbstractMap.SimpleEntry;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private static final JsonValidator underTest = new JsonValidator();

    @Test
    public void shouldPassValidJsonWithOnlyMandatoryFieldSet() throws JsonProcessingException {
        val json = createJsonFromEntries(Collections.singletonList(entry(Fields.MANDATORY, "somevalue")));
        Assert.isEmpty(underTest.validate(json, json, schema));
    }

    @Test
    public void shouldPassValidJsonWithAllFieldsSet() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "somevalue"),
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, 1)
        ));
        Assert.isEmpty(underTest.validate(json, json, schema));
    }

    @Test
    public void shouldPassValidJsonIrrespectiveOfFieldOrdering() throws JsonProcessingException {
        val json = "{\"mandatory\":\"foo\",\"numeric\":1}";
        val jsonWithReverseFieldOrder = "{\"numeric\":1,\"mandatory\":\"foo\"}";
        Assert.isEmpty(underTest.validate(json, jsonWithReverseFieldOrder, schema));
    }

    @Test
    public void shouldFailJsonWithMissingMandatoryValue() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, 1)
        ));
        Assert.isNonEmpty(underTest.validate(json, json, schema));
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

        Assert.isNonEmpty(underTest.validate(json, fakeParsedJson, schema));
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

        Assert.isNonEmpty(underTest.validate(json, fakeParsedJson, schema));
    }

    @Test
    public void shouldPassWhenThereIsNoContent() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "somevalue"),
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, "this is not a number")
        ));

        Assert.isEmpty(underTest.validate(null, json, schema));
        Assert.isEmpty(underTest.validate(json, null, schema));
    }

    private static SimpleEntry<String, Object> entry(String key, Object value) {
        return new SimpleEntry<>(key, value);
    }

    private static String createJsonFromEntries(List<SimpleEntry<String, Object>> entries) throws JsonProcessingException {
            return objectMapper.writeValueAsString(
                    entries.stream()
                            .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue))
            );
    }

}