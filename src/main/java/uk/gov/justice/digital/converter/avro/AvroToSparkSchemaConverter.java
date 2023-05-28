package uk.gov.justice.digital.converter.avro;

import jakarta.inject.Singleton;
import org.apache.avro.Schema;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import uk.gov.justice.digital.converter.Converter;

import java.util.Optional;

/**
 * Avro Schema to Spark Schema (StructType) converter.
 * <p>
 * Makes use of the SchemaConverters class provided by spark-avro.
 * <p>
 * Avro types are converted to Spark types as per the documentation.
 * <p>
 * See <a href="https://spark.apache.org/docs/3.3.0/sql-data-sources-avro.html#supported-types-for-avro---spark-sql-conversion">Avro to Spark Schema Conversion Documentation</a>
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