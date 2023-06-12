package uk.gov.justice.digital.job.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.AbstractMap.SimpleEntry;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonValidatorTest {

    private static final class Fields {
        public static final String MANDATORY = "mandatory";
        public static final String OPTIONAL = "optional";
        public static final String NUMERIC = "numeric";
        public static final String DATE = "date";
        public static final String TIMESTAMP = "timestamp";
    }

    private static final StructType schema =
            new StructType()
                    .add(Fields.MANDATORY, StringType, false)
                    .add(Fields.OPTIONAL, StringType, true)
                    .add(Fields.NUMERIC, IntegerType, true);

    private static final StructType schemaWithDate =
            new StructType()
                    .add(Fields.DATE, DateType, false);

    private static final StructType schemaWithTimestamp =
            new StructType()
                    .add(Fields.TIMESTAMP, TimestampType, false);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final JsonValidator underTest = new JsonValidator();

    @Test
    public void shouldPassValidJsonWithOnlyMandatoryFieldSet() throws JsonProcessingException {
        val json = createJsonFromEntries(Collections.singletonList(entry(Fields.MANDATORY, "somevalue")));
        assertTrue(underTest.validate(json, json, schema));
    }

    @Test
    public void shouldPassValidJsonWithAllFieldsSet() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "somevalue"),
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, 1)
        ));
        assertTrue(underTest.validate(json, json, schema));
    }

    @Test
    public void shouldPassValidJsonIrrespectiveOfFieldOrdering() throws JsonProcessingException {
        val json = "{\"mandatory\":\"foo\",\"numeric\":1}";
        val jsonWithReverseFieldOrder = "{\"numeric\":1,\"mandatory\":\"foo\"}";
        assertTrue(underTest.validate(json, jsonWithReverseFieldOrder, schema));
    }

    @Test
    public void shouldFailJsonWithMissingMandatoryValue() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, 1)
        ));
        assertFalse(underTest.validate(json, json, schema));
    }

    /**
     * This test exercises the specific case where we have used from_json to parse a JSON string against a schema.
     * If a value doesn't match the type specified for the field the value is returned as NULL. For this reason we
     * must compare the original and parsed JSON data since any differences will be down to invalid types.
     */
    @Test
    public void shouldFailWhenParsedJsonDoesNotMatchOriginalJson() throws JsonProcessingException {
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

        assertFalse(underTest.validate(json, fakeParsedJson, schema));
    }

    @Test
    public void shouldPassWhenThereIsNoContent() throws JsonProcessingException {
        val json = createJsonFromEntries(Arrays.asList(
                entry(Fields.MANDATORY, "somevalue"),
                entry(Fields.OPTIONAL, "anotherValue"),
                entry(Fields.NUMERIC, "this is not a number")
        ));

        assertTrue(underTest.validate(null, json, schema));
        assertTrue(underTest.validate(json, null, schema));
    }

    @Test
    public void shouldPassWhenRawDateTimeIsComparedToFromJsonDateTime() throws JsonProcessingException {
        val rawJson = createJsonFromEntries(Collections.singletonList(entry(Fields.TIMESTAMP, "2012-01-01T01:23:45.678901Z")));
        // When spark converts the parsed data back to JSON we only see the millisecond part. See notes in JsonValidator.
        val parsedJson = createJsonFromEntries(Collections.singletonList(entry(Fields.TIMESTAMP, "2012-01-01T01:23:45.678Z")));
        assertTrue(
                underTest.validate(rawJson, parsedJson, schemaWithTimestamp),
                "Validator should ignore precision beyond the first three digits representing milliseconds."
        );
    }

    @Test
    public void shouldPassWhenDateTimeToDateConversionHandlesZeroTimePart() throws JsonProcessingException {
        val rawJson = createJsonFromEntries(Collections.singletonList(entry(Fields.DATE, "2012-01-01T00:00:00.000Z")));
        val parsedJson = createJsonFromEntries(Collections.singletonList(entry(Fields.DATE, "2012-01-01")));

        assertTrue(
                underTest.validate(rawJson, parsedJson, schemaWithDate),
                "Validator should pass when raw string contains a zeroed time part. " +
                        "Raw dates are sent as an ISO datetime but our schema declares a type of date so spark " +
                        "discards the time part."
        );
    }

    @Test
    public void shouldFailWhenDateTimeToDateConversionHandlesTimePartWithValues() throws JsonProcessingException {
        val rawJson = createJsonFromEntries(Collections.singletonList(entry(Fields.DATE, "2012-01-01T12:34:56.789Z")));
        val parsedJson = createJsonFromEntries(Collections.singletonList(entry(Fields.DATE, "2012-01-01")));

        assertFalse(
                underTest.validate(rawJson, parsedJson, schemaWithDate),
                "Validator should fail when raw string contains a time part with non zero values. " +
                        "Raw dates are sent as an ISO datetime but our schema declares a type of date so spark " +
                        "discards the time part. If the time part contains non-zero values the assumption is that " +
                        "we should probably declare a datetime in our avro schema."
        );

    }

    @Test
    public void shouldPassTimestampsThatContainNoFractionalSeconds() throws JsonProcessingException {
        val rawJson = createJsonFromEntries(Collections.singletonList(entry(Fields.TIMESTAMP, "2006-01-13T08:59:09Z")));
        val parsedJson = createJsonFromEntries(Collections.singletonList(entry(Fields.TIMESTAMP, "2006-01-13T08:59:09.000Z")));

        assertTrue(underTest.validate(rawJson, parsedJson, schemaWithTimestamp));
    }

    @Test
    public void shouldPassWhenRawDateContainsNoTimePart() throws JsonProcessingException {
        val rawJson = createJsonFromEntries(Collections.singletonList(entry(Fields.DATE, "2012-01-01")));
        val parsedJson = createJsonFromEntries(Collections.singletonList(entry(Fields.DATE, "2012-01-01")));

        assertTrue(
                underTest.validate(rawJson, parsedJson, schemaWithDate),
                "Validator should pass when raw string contains a zeroed time part. " +
                        "Raw dates are sent as an ISO datetime but our schema declares a type of date so spark " +
                        "discards the time part."
        );
    }

    @Test
    public void shouldFailGracefullyGivenAnInvalidDateValue() throws JsonProcessingException {
        val rawJson = createJsonFromEntries(Collections.singletonList(entry(Fields.DATE, "fooTbar")));
        // Parsing the invalid string as a Date would yield a null in Spark as per the JSON below.
        val parsedJson = "{\"date\":null}";

        assertFalse(
                underTest.validate(rawJson, parsedJson, schemaWithDate),
                "Validator should fail when raw string contains an invalid value."
        );
    }

    @Test
    public void shouldFailGracefullyGivenAnInvalidTimestampValue() throws JsonProcessingException {
        val rawJson = createJsonFromEntries(Collections.singletonList(entry(Fields.TIMESTAMP, "fooTbar")));
        // Parsing the invalid string as a Timestamp would yield a null in Spark as per the JSON below.
        val parsedJson = "{\"timestamp\":null}";

        assertFalse(
                underTest.validate(rawJson, parsedJson, schemaWithTimestamp),
                "Validator should fail when raw string contains an invalid value."
        );
    }

    @Test
    public void shouldHandleADateFieldThatIsNull() throws JsonProcessingException {
        val rawJson = "{\"date\":null}";
        val parsedJson = "{\"date\":null}";

        assertFalse(
                underTest.validate(rawJson, parsedJson, schemaWithDate),
                "Validator should fail when raw string contains an invalid value."
        );
    }

    @Test
    public void shouldAccountForReducedPrecisonOfSparkDateTime() {
//        2009-08-02T04:48:27.792046000Z, 2009-08-02T04:48:27.792Z
        val datetime = ZonedDateTime.parse("2009-08-02T04:48:27.792046000Z");
        System.out.println("Got datetime: " + datetime);
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