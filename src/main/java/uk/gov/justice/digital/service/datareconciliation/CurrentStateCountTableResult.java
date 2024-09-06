package uk.gov.justice.digital.service.datareconciliation;

import lombok.Data;

@Data
public class CurrentStateCountTableResult {
    private final long nomisCount;
    private final long structuredCount;
    private final long curatedCount;
    // TODO Add optional Operational DataStore count

    public boolean countsMatch() {
        return structuredCount == curatedCount && nomisCount == curatedCount;
    }

    public String summary() {
        return (countsMatch() ? "MATCH: " : "MISMATCH: ") +
                "Nomis count: " + nomisCount + ", Structured Zone: " + structuredCount + ", Curated Zone: " + curatedCount;
    }
}
