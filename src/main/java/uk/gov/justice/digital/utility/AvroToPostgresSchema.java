package uk.gov.justice.digital.utility;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

public class AvroToPostgresSchema {

    private static final Map<Schema.Type, String> AVRO_TO_POSTGRES_TYPE = new EnumMap<>(Schema.Type.class);

    static {
        AVRO_TO_POSTGRES_TYPE.put(Schema.Type.STRING, "text");
        AVRO_TO_POSTGRES_TYPE.put(Schema.Type.BOOLEAN, "boolean");
        AVRO_TO_POSTGRES_TYPE.put(Schema.Type.INT, "integer");
        AVRO_TO_POSTGRES_TYPE.put(Schema.Type.LONG, "bigint");
        AVRO_TO_POSTGRES_TYPE.put(Schema.Type.FLOAT, "real");
        AVRO_TO_POSTGRES_TYPE.put(Schema.Type.DOUBLE, "double precision");

    }

    @SuppressWarnings({"java:S112", "java:S106"})
    public static void main(String[] args) throws IOException {
        String avroSchemaFile = args[0];
        String tableName = args[1];

        Schema schema = new Schema.Parser().parse(new File(avroSchemaFile));

        StringBuilder createTable = new StringBuilder("CREATE TABLE ");
        createTable.append(tableName);
        createTable.append(" (\n");
        for (Schema.Field field : schema.getFields()) {
            createTable.append("    ");
            createTable.append(field.name().toLowerCase());
            createTable.append(" ");

            Schema.Type type = field.schema().getType();
            LogicalType logicalType = field.schema().getLogicalType();

            if (logicalType instanceof LogicalTypes.Decimal) {
                int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
                int scale = ((LogicalTypes.Decimal) logicalType).getScale();
                createTable.append("numeric(");
                createTable.append(precision);
                createTable.append(",");
                createTable.append(scale);
                createTable.append(")");
            } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
                createTable.append("timestamp");
            } else {
                String postgresType = AVRO_TO_POSTGRES_TYPE.get(type);
                if (postgresType == null) {
                    throw new RuntimeException("Not implemented yet for logical type: " + logicalType + " and type: " + type);
                } else {
                    createTable.append(postgresType);
                }
            }
            createTable.append(",\n"); // Need to manually strip the last ','

        }

        createTable.append(");");

        System.out.println(createTable);
    }
}
