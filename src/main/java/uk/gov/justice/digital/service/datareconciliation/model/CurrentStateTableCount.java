package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Data;

import static uk.gov.justice.digital.service.datareconciliation.ReconciliationTolerance.equalWithTolerance;

/**
 * Represents the results of running the total counts data reconciliation for a single "current state" table in DataHub
 * replicated across multiple datastores
 */
@Data
public class CurrentStateTableCount {
    private final double relativeTolerance;
    private final long absoluteTolerance;

    private final long dataSourceCount;
    private final long curatedCount;
    // A null value indicates that there is no Operational DataStore count because this table is not stored in the
    // Operational Data Store whereas it would have a value of zero if there was an issue reading the count.
    private final Long operationalDataStoreCount;

    public CurrentStateTableCount(double relativeTolerance, long absoluteTolerance, long dataSourceCount, long curatedCount, Long operationalDataStoreCount) {
        this.relativeTolerance = relativeTolerance;
        this.absoluteTolerance = absoluteTolerance;
        this.dataSourceCount = dataSourceCount;
        this.curatedCount = curatedCount;
        this.operationalDataStoreCount = operationalDataStoreCount;
    }

    public CurrentStateTableCount(double relativeTolerance, long absoluteTolerance, long dataSourceCount, long curatedCount) {
        this.relativeTolerance = relativeTolerance;
        this.absoluteTolerance = absoluteTolerance;
        this.dataSourceCount = dataSourceCount;
        this.curatedCount = curatedCount;
        this.operationalDataStoreCount = null;
    }

    public boolean countsMatch() {
        boolean dataSourceMatchesCurated = equalWithTolerance(dataSourceCount, curatedCount, absoluteTolerance, relativeTolerance);
        boolean operationalDataStoreSkippedOrMatchesCurated = operationalDataStoreCount == null || equalWithTolerance(operationalDataStoreCount, curatedCount, absoluteTolerance, relativeTolerance);

        return dataSourceMatchesCurated && operationalDataStoreSkippedOrMatchesCurated;
    }

    private boolean countsExactMatch() {
        boolean dataSourceMatchesCurated = dataSourceCount == curatedCount;
        boolean operationalDataStoreSkippedOrMatchesCurated = operationalDataStoreCount == null || operationalDataStoreCount == curatedCount;

        return dataSourceMatchesCurated && operationalDataStoreSkippedOrMatchesCurated;
    }

    public String summary() {
        String matchMessage;
        if (countsExactMatch()) {
            matchMessage = "MATCH";
        } else if (countsMatch()) {
            matchMessage = "MATCH (within tolerance)";
        } else {
            matchMessage = "MISMATCH";
        }
        return "Data Source: " + dataSourceCount + ", Curated Zone: " + curatedCount
                +", Operational DataStore: " + (operationalDataStoreCount == null ? "skipped": operationalDataStoreCount) +
                "\t - " + matchMessage;
    }

}
