package uk.gov.justice.digital.job.model.dms;

import lombok.*;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;

@Data
@Builder
@Jacksonized
public class EventMetadata {
    private Instant timestamp;
    private String recordType;
    private String operation;
    private String partitionKeyType;
    private String partitionKeyValue;
    private String schemaName;
    private String tableName;
}
