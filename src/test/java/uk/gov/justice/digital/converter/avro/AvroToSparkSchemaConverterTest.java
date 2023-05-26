package uk.gov.justice.digital.converter.avro;

import lombok.val;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap.SimpleEntry;
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
                new SimpleEntry<>("bytes",   DataTypes.ByteType),
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
            assertEquals(expectedSchema, result);
        });
    }

    @Test
    public void shouldConvertAllLogicalFieldTypesToCorrectSparkType() {
        val typeMappings = Stream.of(
                new SimpleEntry<>("date",             DataTypes.DateType),
                new SimpleEntry<>("timestamp-millis", DataTypes.TimestampType),
                new SimpleEntry<>("timestamp-micros", DataTypes.TimestampType),
                new SimpleEntry<>("time-micros",      DataTypes.TimestampType),
                new SimpleEntry<>("time-millis",      DataTypes.TimestampType)
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        typeMappings.forEach((avroType, sparkType) -> {
            val avroSchema = avroSchemaWithLogicalType(avroType);
            val expectedSchema = sparkSchemaWithType(sparkType);
            val result = underTest.convert(avroSchema);
            assertEquals(expectedSchema, result);
        });
    }

    private String avroSchemaWithSimpleType(String type) {
        val fakeSchema = "{ " +
                "\"fields\": [{ " +
                    "\"name\": \"aField\", " +
                    "\"type\": \"TYPE\", " +
                    "\"nullable\": false " +
                " }]} ";
       return fakeSchema.replace("TYPE", type);
    }

    private String avroSchemaWithLogicalType(String type) {
        val fakeSchema = "{ " +
                "\"fields\": [{ " +
                    "\"name\": \"aField\", " +
                    "\"type\": { " +
                        "\"type\": \"int\", " +
                        "\"logicalType\": \"TYPE\" " +
                    "}, " +
                    "\"nullable\": false " +
                " }]} ";
        return fakeSchema.replace("TYPE", type);
    }

    private StructType sparkSchemaWithType(DataType type) {
        return new StructType().add("aField", type, false);
    }

}