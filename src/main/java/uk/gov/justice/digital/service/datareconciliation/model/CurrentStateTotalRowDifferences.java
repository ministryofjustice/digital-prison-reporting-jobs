package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.val;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the results of running the row differences reconciliation for the "current state" data in DataHub
 * for multiple tables.
 */
@EqualsAndHashCode
@ToString
public class CurrentStateTotalRowDifferences implements DataReconciliationResult {

    private final Map<String, CurrentStateTableRowDifferences> tableToDifferences = new HashMap<>();

    public void put(String fullTableName, CurrentStateTableRowDifferences rowDifferences) {
        tableToDifferences.put(fullTableName, rowDifferences);
    }

    /**
     * Returns the value to which the specified key is mapped, or
     *  {@code null} if this map contains no mapping for the key
     */
    public CurrentStateTableRowDifferences get(String fullTableName) {
        return tableToDifferences.get(fullTableName);
    }

    @Override
    public boolean isSuccess() {
        return tableToDifferences.values().stream().allMatch(CurrentStateTableRowDifferences::rowsMatch);
    }

    @Override
    public String summary() {
        StringBuilder sb = new StringBuilder("Current State Row Differences ");
        if (isSuccess()) {
            sb.append("MATCH:\n");
        } else {
            sb.append("DO NOT MATCH:\n");
        }

        for (val entrySet: tableToDifferences.entrySet()) {
            val tableName = entrySet.getKey();
            val differences = entrySet.getValue();
            sb.append("For table ").append(tableName).append(":\n");
            sb.append("\t").append(differences.summary()).append("\n");
        }

        sb.append("\n").append("Mismatching primary keys:\n");

        for (val entrySet: tableToDifferences.entrySet()) {
            val tableName = entrySet.getKey();
            val differences = entrySet.getValue();
            sb.append("For table ").append(tableName).append(":\n");
            sb.append("\t").append(differences.details()).append("\n");
        }

        return sb.toString();
    }
}
