package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Getter;

/**
 * Represents the results of running the total counts data reconciliation for a single "current state" table in DataHub
 * replicated across multiple datastores
 */
@Getter
public class CurrentStateTableCount {
    private final long nomisCount;
    private final long structuredCount;
    private final long curatedCount;
    // A null value indicates that there is no Operational DataStore count
    private final Long operationalDataStoreCount;

    public CurrentStateTableCount(long nomisCount, long structuredCount, long curatedCount, long operationalDataStoreCount) {
        this.nomisCount = nomisCount;
        this.structuredCount = structuredCount;
        this.curatedCount = curatedCount;
        this.operationalDataStoreCount = operationalDataStoreCount;
    }

    public CurrentStateTableCount(long nomisCount, long structuredCount, long curatedCount) {
        this.nomisCount = nomisCount;
        this.structuredCount = structuredCount;
        this.curatedCount = curatedCount;
        this.operationalDataStoreCount = null;
    }

    public boolean countsMatch() {
        boolean structuredMatchesCurated = structuredCount == curatedCount;
        boolean nomisMatchesCurated = nomisCount == curatedCount;
        boolean operationalDataStoreSkippedOrMatchesCurated = operationalDataStoreCount == null || operationalDataStoreCount == curatedCount;

        return structuredMatchesCurated && nomisMatchesCurated && operationalDataStoreSkippedOrMatchesCurated;
    }

    public String summary() {
        return (countsMatch() ? "   MATCH: " : "MISMATCH: ") +
                "Nomis: " + nomisCount + ", Structured Zone: " + structuredCount + ", Curated Zone: " + curatedCount
                +", Operational DataStore: " + (operationalDataStoreCount == null ? "skipped": operationalDataStoreCount);
    }


}
