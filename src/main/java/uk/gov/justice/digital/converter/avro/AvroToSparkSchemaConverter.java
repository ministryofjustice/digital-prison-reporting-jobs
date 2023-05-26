package uk.gov.justice.digital.converter.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.spark.sql.types.*;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Singleton
public class AvroToSparkSchemaConverter {

    // TODO - revert to new SimpleEntry<> as per the tests - mapEntry isn't really adding much
    private static final Map<String, DataType> avroSimpleTypeNameToSparkType = Stream.of(
            mapEntry(Schema.Type.BOOLEAN,   DataTypes.BooleanType),
            mapEntry(Schema.Type.BYTES,     DataTypes.ByteType),
            mapEntry(Schema.Type.DOUBLE,    DataTypes.DoubleType),
            mapEntry(Schema.Type.FLOAT,     DataTypes.FloatType),
            mapEntry(Schema.Type.INT,       DataTypes.IntegerType),
            mapEntry(Schema.Type.LONG,      DataTypes.LongType),
            mapEntry(Schema.Type.NULL,      DataTypes.NullType),
            mapEntry(Schema.Type.STRING,    DataTypes.StringType)
    ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    private static final Map<String, DataType> avroLogicalTypeNameToSparkType = Stream.of(
            mapEntry(LogicalTypes.date(),               DataTypes.DateType),
            mapEntry(LogicalTypes.timestampMicros(),    DataTypes.TimestampType),
            mapEntry(LogicalTypes.timestampMillis(),    DataTypes.TimestampType),
            // TODO - there is no time type in spark so for now we use the timestamp type.
            //      - conversion will need to be handled later during the normalisation stage.
            mapEntry(LogicalTypes.timeMillis(),         DataTypes.TimestampType),
            mapEntry(LogicalTypes.timeMicros(),         DataTypes.TimestampType)
    ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public StructType convert(String avroSchema) {
        try {
            val parsedSchema = objectMapper.readTree(avroSchema);
            val avroFields = parsedSchema.get("fields").elements();
            return buildStructTypeFromAvroFields(avroFields);
        }
        catch (JsonProcessingException jpe) {
            // TODO - define a custom RTE e.g. SchemaConverterError?
            throw new RuntimeException("Unexpected error when processing schema", jpe);
        }
    }

    private static SimpleEntry<String, DataType> mapEntry(Schema.Type avroType, DataType sparkType) {
        return new SimpleEntry<>(avroType.getName(), sparkType);
    }

    private static SimpleEntry<String, DataType> mapEntry(LogicalType avroType, DataType sparkType) {
        return new SimpleEntry<>(avroType.getName(), sparkType);
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
        val nullable = node.get("nullable").asBoolean();
        val typeNode = node.get("type");
        val type = (typeNode.isObject())
                ? typeNode.get("logicalType").asText()
                : typeNode.asText();

        if (isSimpleType(type)) {
            return new StructField(
                    name,
                    avroSimpleTypeNameToSparkType.get(type),
                    nullable,
                    Metadata.empty());
        }
        else if (isEnumType(type)) {
            throw new RuntimeException("ENUM handling ot implemented yet");
        }
        else if (isLogicalType(type)) {
            return new StructField(
                    name,
                    avroLogicalTypeNameToSparkType.get(type),
                    nullable,
                    Metadata.empty());
        }
        else throw new RuntimeException("Unsupported avro field type: " + type);
    }

    private boolean isSimpleType(String type) {
        return avroSimpleTypeNameToSparkType.containsKey(type);
    }

    private boolean isEnumType(String type) {
        return Schema.Type.ENUM.getName().equals(type);
    }

    private boolean isLogicalType(String logicalType) {
        return avroLogicalTypeNameToSparkType.containsKey(logicalType);
    }

}
