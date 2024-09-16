package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.val;

/**
 * Represents the results of running the total counts data reconciliation for the "current state" data in DataHub
 */
@EqualsAndHashCode
@ToString
public class CurrentStateTotalCounts {

    private final CountsByTable<CurrentStateTableCount> tableToResult = new CountsByTable<>();

    public void put(String fullTableName, CurrentStateTableCount currentStateTableCount) {
        tableToResult.put(fullTableName, currentStateTableCount);
    }

    /**
     * Returns the value to which the specified key is mapped, or
     *  {@code null} if this map contains no mapping for the key
     */
    public CurrentStateTableCount get(String fullTableName) {
        return tableToResult.get(fullTableName);
    }

    // todo replace with countsMatch
    public boolean isFailure() {
        return tableToResult.entrySet().stream().anyMatch(entry -> !entry.getValue().countsMatch());
    }

    public String summary() {
        StringBuilder sb = new StringBuilder("Current State Total Counts ");
        if (isFailure()) {
            sb.append("DO NOT MATCH:\n");
        } else {
            sb.append("MATCH:\n");
        }

        for (val entrySet: tableToResult.entrySet()) {
            val tableName = entrySet.getKey();
            val currentStateCountTableResult = entrySet.getValue();
            sb.append("For table ").append(tableName).append(":\n");
            sb.append("\t").append(currentStateCountTableResult.summary()).append("\n");
        }

        return sb.toString();
    }
}
