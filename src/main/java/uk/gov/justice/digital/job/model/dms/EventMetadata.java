package uk.gov.justice.digital.job.model.dms;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder
@Jacksonized
public class EventMetadata {
    private String timestamp;
    private String recordType;
    private String operation;
    private String partitionKeyType;
    private String partitionKeyValue;
    private String schemaName;
    private String tableName;
}
