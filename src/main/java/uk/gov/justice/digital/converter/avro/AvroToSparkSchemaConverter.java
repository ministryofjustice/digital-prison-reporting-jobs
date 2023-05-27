package uk.gov.justice.digital.converter.avro;

import jakarta.inject.Singleton;
import org.apache.avro.Schema;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

@Singleton
public class AvroToSparkSchemaConverter {

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
        return new Schema.Parser().parse(avro);
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

}