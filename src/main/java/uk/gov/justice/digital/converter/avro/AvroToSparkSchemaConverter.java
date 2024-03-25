package uk.gov.justice.digital.converter.avro;

import jakarta.inject.Singleton;
import lombok.val;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import uk.gov.justice.digital.converter.Converter;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Avro Schema to Spark Schema (StructType) converter.
 * <p>
 * Makes use of the SchemaConverters class provided by spark-avro.
 * <p>
 * Avro types are converted to Spark types as per the documentation.
 * <p>
 * See <a href="https://spark.apache.org/docs/3.3.0/sql-data-sources-avro.html#supported-types-for-avro---spark-sql-conversion">Avro to Spark Schema Conversion Documentation</a>
 * <p>
 * Note - our custom `nullable` field property is converted to the correct avro representation by the schema converter
 *        prior to conversion to the Spark Schema ensuring that nullable fields are represented correctly after
 *        conversion.
 */
@Singleton
public class AvroToSparkSchemaConverter implements Converter<String, StructType> {

    @Override
    public StructType convert(String avroSchemaString) {
        return Optional.of(avroSchemaString)
                .map(this::toAvroSchema)
                .map(this::toSparkDataType)
                .flatMap(this::castToStructType)
                .orElseThrow(() ->
                        new IllegalArgumentException("Unable to cast DataType to StructType schema: '" + avroSchemaString + "'")
                );
    }

    private Schema toAvroSchema(String avro) {
        // The Avro Schema Parser is stateful and will cache parsed schemas. We choose not to benefit from this since
        // the parser also prevents an existing definition from being updated and will throw a SchemaParseException.
        // This allows us to handle new versions of a schema as and when they are published.
        val parsedAvroSchema = new Schema.Parser().parse(avro);
        // Convert time fields to timestamp because Spark does not support time data type.
        val convertedFields = convertTimeFieldsToTimestamp(parsedAvroSchema);

        return Schema.createRecord(
                parsedAvroSchema.getName(),
                parsedAvroSchema.getDoc(),
                parsedAvroSchema.getNamespace(),
                parsedAvroSchema.isError(),
                convertedFields
        );
    }

    private DataType toSparkDataType(Schema avroSchema) {
        return SchemaConverters
                .toSqlType(avroSchema)
                .dataType();
    }

    private Optional<StructType> castToStructType(DataType sparkDataType) {
        return Optional.of(sparkDataType)
                .filter(StructType.class::isInstance)
                .map(StructType.class::cast);
    }

    private List<Schema.Field> convertTimeFieldsToTimestamp(Schema avroSchema) {
        return avroSchema.getFields().stream()
                .map(this::convertTimeToTimestamp)
                .collect(Collectors.toList());
    }

    private Schema.Field convertTimeToTimestamp(Schema.Field avroField) {
        boolean isIntegerField = avroField.schema().getType().getName().equalsIgnoreCase(Schema.Type.INT.name());
        boolean logicalTypeIsTimeMillis = avroField.schema().getLogicalType() instanceof LogicalTypes.TimeMillis;
        boolean isTimeField = isIntegerField && logicalTypeIsTimeMillis;

        return isTimeField ? toTimestampField(avroField) : new Schema.Field(
                avroField.name(),
                isNullable(avroField) ? nullableFieldSchema(avroField.schema()) : avroField.schema(),
                avroField.doc(),
                (avroField.hasDefaultValue()) ? avroField.defaultVal() : null
        );
    }

    private Schema.Field toTimestampField(Schema.Field avroField) {
        Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        return new Schema.Field(
                avroField.name(),
                isNullable(avroField) ? nullableFieldSchema(timestampSchema) : timestampSchema,
                avroField.doc(),
                (avroField.hasDefaultValue()) ? avroField.defaultVal() : null
        );
    }

    @NotNull
    private static Schema nullableFieldSchema(Schema fieldSchema) {
        // Ensure our custom nullable: true|false field is represented in the correct avro form (union type with null) so
        // that conversion to Spark results in a StructType with the correct nullability properties.
        return Schema.createUnion(fieldSchema, Schema.create(Schema.Type.NULL));
    }

    @NotNull
    private static Boolean isNullable(Schema.Field avroField) {
        return Optional.ofNullable(avroField.getObjectProp("nullable"))
                .map(nullable -> nullable.equals(true))
                .orElse(false);
    }

}