package uk.gov.justice.digital.converter.avro;

import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroToSparkSchemaConverterIntegrationTest {

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
            val avroSchema = avroSchemaWithLogicalType(avroType, false);
            val expectedSchema = sparkSchemaWithType(sparkType);
            val result = underTest.convert(avroSchema);
            assertEquals(expectedSchema, result, String.format("Failed to convert avro: %s to spark: %s", avroType, sparkType));
        });
    }

    @Test
    public void shouldConvertAvroEnumToSparkEnum() {
        val avroEnumSchema = SchemaBuilder.record("test")
                .fields()
                    .name("aField")
                    .type()
                        .enumeration("things")
                        .symbols("foo", "bar", "baz")
                    .noDefault()
                .endRecord();

        // Avro enums are converted to non-nullable String fields.
        val sparkEnumSchema = new StructType()
                .add("aField", DataTypes.StringType, false);

        assertEquals(sparkEnumSchema, underTest.convert(avroEnumSchema.toString()));
    }

    @Test
    public void shouldPreserveFieldNullabilityAttributes() {
        val avro = SchemaBuilder.record("test")
                .fields()
                    .name("anOptionalField")
                    .prop("nullable", true)
                    .type("string")
                    .noDefault()
                    .name("aRequiredField")
                    .prop("nullable", false)
                    .type("string")
                    .noDefault()
                .endRecord()
                .toString();

        val sparkSchema = new StructType()
                .add("anOptionalField", DataTypes.StringType, true)
                .add("aRequiredField", DataTypes.StringType, false);

        assertEquals(sparkSchema, underTest.convert(avro));
    }

    @Test
    public void shouldPreserveFieldNullabilityAttributesForLogicalTypes() {
        val nullable = true;
        val avroSchema = avroSchemaWithLogicalType("date", nullable);
        val sparkSchema = new StructType()
                .add("aField", DataTypes.DateType, nullable);

        assertEquals(sparkSchema, underTest.convert(avroSchema));
    }

    @Test
    public void shouldIncludeMetadataAfterConversion() {
        val avroSchemaString = "{" +
                "  \"type\": \"record\"," +
                "  \"name\": \"test\"," +
                "  \"fields\": [" +
                "    {" +
                "      \"name\": \"aField\"," +
                "      \"type\": \"string\"," +
                "      \"metadata\": {" +
                "        \"validationType\": \"time\"" +
                "      }" +
                "    }" +
                "  ]" +
                "}";
        
        val result = underTest.convert(avroSchemaString);
        
        assertTrue(result.fields()[0].metadata().contains("validationType"));
    }


    private String avroSchemaWithSimpleType(String type) {
        return SchemaBuilder.record("test")
                .fields()
                    .name("aField")
                    .type(type)
                    .noDefault()
                .endRecord()
                .toString();
    }

    private String avroSchemaWithLogicalType(String logicalType, boolean isNullable) {
        return SchemaBuilder.record("test")
                .fields()
                    .name("aField")
                    .prop("nullable", isNullable)
                    .type(logicalTypeSchema(logicalType))
                    .noDefault()
                .endRecord()
                .toString(true);
    }

    private Schema logicalTypeSchema(String logicalType) {
        val intTypes = Arrays.asList("date", "time-millis");
        val internalType = (intTypes.contains(logicalType)) ? "int" : "long";
        val logicalTypeSchema = SchemaBuilder.builder().type(internalType);
        logicalTypeSchema.addProp("logicalType", logicalType);
        return logicalTypeSchema;
    }

    private StructType sparkSchemaWithType(DataType type) {
        val isNullable = type == DataTypes.NullType;
        return new StructType().add("aField", type, isNullable);
    }

}