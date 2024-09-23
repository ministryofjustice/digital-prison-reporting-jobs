package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Data;

/**
 * Represents the results of running the total counts data reconciliation for a single "current state" table in DataHub
 * replicated across multiple datastores
 */
@Data
public class CurrentStateTableCount {
    private final long dataSourceCount;
    private final long structuredCount;
    private final long curatedCount;
    // A null value indicates that there is no Operational DataStore count
    private final Long operationalDataStoreCount;

    public CurrentStateTableCount(long dataSourceCount, long structuredCount, long curatedCount, Long operationalDataStoreCount) {
        this.dataSourceCount = dataSourceCount;
        this.structuredCount = structuredCount;
        this.curatedCount = curatedCount;
        this.operationalDataStoreCount = operationalDataStoreCount;
    }

    public CurrentStateTableCount(long dataSourceCount, long structuredCount, long curatedCount) {
        this.dataSourceCount = dataSourceCount;
        this.structuredCount = structuredCount;
        this.curatedCount = curatedCount;
        this.operationalDataStoreCount = null;
    }

    public boolean countsMatch() {
        boolean structuredMatchesCurated = structuredCount == curatedCount;
        boolean nomisMatchesCurated = dataSourceCount == curatedCount;
        boolean operationalDataStoreSkippedOrMatchesCurated = operationalDataStoreCount == null || operationalDataStoreCount == curatedCount;

        return structuredMatchesCurated && nomisMatchesCurated && operationalDataStoreSkippedOrMatchesCurated;
    }

    public String summary() {
        return "Data Source: " + dataSourceCount + ", Structured Zone: " + structuredCount + ", Curated Zone: " + curatedCount
                +", Operational DataStore: " + (operationalDataStoreCount == null ? "skipped": operationalDataStoreCount) +
                (countsMatch() ? "\t - MATCH" : "\t - MISMATCH");
    }


}
