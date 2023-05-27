package uk.gov.justice.digital.converter.avro;

import lombok.val;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class AvroToSparkSchemaConverterTest {

    private static final AvroToSparkSchemaConverter underTest = new AvroToSparkSchemaConverter();

    @Test
    public void shouldConvertAllSimpleFieldTypesToCorrectSparkType() {
        val typeMappings = Stream.of(
                new SimpleEntry<>("boolean", DataTypes.BooleanType),
                new SimpleEntry<>("bytes",   DataTypes.BinaryType),
                new SimpleEntry<>("double",  DataTypes.DoubleType),
                new SimpleEntry<>("float",   DataTypes.FloatType),
                new SimpleEntry<>("int",     DataTypes.IntegerType),
                new SimpleEntry<>("long",    DataTypes.LongType),
                new SimpleEntry<>("null",    DataTypes.NullType),
                new SimpleEntry<>("string",  DataTypes.StringType)
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        typeMappings.forEach((avroType, sparkType) -> {
            val avroSchema = avroSchemaWithSimpleType(avroType);
            val expectedSchema = sparkSchemaWithType(sparkType);
            val result = underTest.convert(avroSchema);
            assertEquals(expectedSchema, result, String.format("Failed to convert avro: %s to spark: %s", avroType, sparkType));
        });
    }

    @Test
    public void shouldConvertAllLogicalFieldTypesToCorrectSparkType() {
        val typeMappings = Stream.of(
                new SimpleEntry<>("date",             DataTypes.DateType),
                new SimpleEntry<>("timestamp-millis", DataTypes.TimestampType),
                new SimpleEntry<>("timestamp-micros", DataTypes.TimestampType),
                // Note - spark does not support these types so the schema converter retains the original avro type.
                new SimpleEntry<>("time-micros",      DataTypes.LongType),
                new SimpleEntry<>("time-millis",      DataTypes.IntegerType)
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        typeMappings.forEach((avroType, sparkType) -> {
            val avroSchema = avroSchemaWithLogicalType(avroType);
            val expectedSchema = sparkSchemaWithType(sparkType);
            val result = underTest.convert(avroSchema);
            assertEquals(expectedSchema, result, String.format("Failed to convert avro: %s to spark: %s", avroType, sparkType));
        });
    }

    private String avroSchemaWithSimpleType(String type) {
        val fakeSchema = "{ " +
                "\"name\": \"test\"," +
                "\"type\": \"record\"," +
                "\"fields\": [{ " +
                    "\"name\": \"aField\", " +
                    "\"type\": \"TYPE\", " +
                    "\"nullable\": false " +
                " }]} ";
       return fakeSchema.replace("TYPE", type);
    }

    private String avroSchemaWithLogicalType(String type) {
        val fakeSchema = "{ " +
                "\"name\": \"test\"," +
                "\"type\": \"record\"," +
                "\"fields\": [{ " +
                    "\"name\": \"aField\", " +
                    "\"type\": { " +
                        "\"type\": \"INTERNAL_TYPE\", " +
                        "\"logicalType\": \"TYPE\" " +
                    "}, " +
                    "\"nullable\": false " +
                " }]} ";

        val intTypes = Arrays.asList("date", "time-millis");
        val internalType = (intTypes.contains(type)) ? "int" : "long";

        return fakeSchema
                .replace("INTERNAL_TYPE", internalType)
                .replace("TYPE", type);
    }

    private StructType sparkSchemaWithType(DataType type) {
        val isNullable = type == DataTypes.NullType;
        return new StructType().add("aField", type, isNullable);
    }

}