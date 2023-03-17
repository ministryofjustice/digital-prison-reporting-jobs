package uk.gov.justice.digital.service.model;

import lombok.Data;
import org.apache.spark.sql.types.DataType;

@Data
public class SourceReference {
    private final String key;
    private final String source;
    private final String table;
    private final String primaryKey;
    private final DataType schema;
}
