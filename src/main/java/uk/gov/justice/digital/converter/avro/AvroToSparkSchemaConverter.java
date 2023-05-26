package uk.gov.justice.digital.converter.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AvroToSparkSchemaConverter {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public StructType convert(String avroSchema) throws JsonProcessingException {
        val parsedSchema = objectMapper.readTree(avroSchema);
        val avroFields = parsedSchema.get("fields").elements();
        return buildStructTypeFromAvroFields(avroFields);
    }

    private StructType buildStructTypeFromAvroFields(Iterator<JsonNode> fields) {
        val structFieldArray =
                toStream(fields)
                        .map(this::toStructField)
                        .toArray(StructField[]::new);

        return new StructType(structFieldArray);
    }

    private <T> Stream<T> toStream(Iterator<T> i) {
        Iterable<T> iterable = () -> i;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private StructField toStructField(JsonNode node) {
        val name = node.get("name").asText();
        val type = node.get("type").asText();
        val nullable = node.get("nullable").asBoolean();

        if (type.equals("string")) {
            return new StructField(
                    name,
                    DataTypes.StringType,
                    nullable,
                    Metadata.empty());
        }
        else throw new RuntimeException("Unsupported avro field type: " + type);
    }

}
