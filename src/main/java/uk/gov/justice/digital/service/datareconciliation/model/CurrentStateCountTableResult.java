package uk.gov.justice.digital.service.datareconciliation.model;

public class CurrentStateCountTableResult {
    private final long nomisCount;
    private final long structuredCount;
    private final long curatedCount;
    // A null value indicates that there is no Operational DataStore count
    private final Long operationalDataStoreCount;

    public CurrentStateCountTableResult(long nomisCount, long structuredCount, long curatedCount, long operationalDataStoreCount) {
        this.nomisCount = nomisCount;
        this.structuredCount = structuredCount;
        this.curatedCount = curatedCount;
        this.operationalDataStoreCount = operationalDataStoreCount;
    }

    public CurrentStateCountTableResult(long nomisCount, long structuredCount, long curatedCount) {
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
        return (countsMatch() ? "MATCH: " : "MISMATCH: ") +
                "Nomis count: " + nomisCount + ", Structured Zone: " + structuredCount + ", Curated Zone: " + curatedCount
                +", Operational DataStore: " + (operationalDataStoreCount == null ? "skipped": operationalDataStoreCount);
    }


}
