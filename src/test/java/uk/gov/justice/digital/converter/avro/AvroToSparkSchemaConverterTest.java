package uk.gov.justice.digital.converter.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.val;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AvroToSparkSchemaConverterTest {

    private static final AvroToSparkSchemaConverter underTest = new AvroToSparkSchemaConverter();

    @Test
    public void shouldConvertAValidSchemaWithASimpleTypeDeclaration() throws JsonProcessingException {

        val fakeSchema = "{ " +
                "\"fields\": [ " +
                    "{ " +
                    "\"name\": \"some-field\", " +
                    "\"type\": \"string\", " +
                    "\"nullable\": false " +
                    " }]} ";

        val result = underTest.convert(fakeSchema);
        val expectedSchema = new StructType().add("some-field", "string", false);

        assertEquals(expectedSchema, result);
    }

}