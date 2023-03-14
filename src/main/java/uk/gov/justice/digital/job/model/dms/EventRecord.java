package uk.gov.justice.digital.job.model.dms;

import lombok.*;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;

@Data
@Builder
@Jacksonized
public class EventRecord {
    private final Map<String, String> data; // For now we do not attempt to parse this field
    private final EventMetadata metadata;
}
