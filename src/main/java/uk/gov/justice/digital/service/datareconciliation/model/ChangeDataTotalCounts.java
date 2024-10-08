package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChangeDataTotalCounts implements DataReconciliationResult {

    private static final String MISSING_COUNTS_MESSAGE = "MISSING COUNTS";

    private Map<String, ChangeDataTableCount> rawZoneCounts;
    private Map<String, ChangeDataTableCount> dmsCounts;
    private Map<String, ChangeDataTableCount> dmsAppliedCounts;

    @Override
    public boolean isSuccess() {
        return sameTables() && rawCountsMatchDmsCounts();
    }

    @Override
    public String summary() {
        StringBuilder sb = new StringBuilder("Change Data Total Counts ");
        if (isSuccess()) {
            sb.append("MATCH:\n\n");
        } else {
            sb.append("DO NOT MATCH:\n\n");
        }

        if (!sameTables()) {
            sb.append("\nThe set of tables for DMS vs Raw zone DO NOT MATCH\n\n");
            String dmsTables = sortedListOfTables(dmsCounts.keySet());
            String dmsAppliedTables = sortedListOfTables(dmsAppliedCounts.keySet());
            String rawTables = sortedListOfTables(rawZoneCounts.keySet());
            sb.append("DMS Tables: ").append(dmsTables).append("\n");
            sb.append("DMS Applied Tables: ").append(dmsAppliedTables).append("\n");
            sb.append("Raw Zone/Raw Archive Tables: ").append(rawTables).append("\n");
            sb.append("\n");
        }

        dmsCounts.forEach((tableName, dmsCount) -> {
            ChangeDataTableCount dmsAppliedCount = dmsAppliedCounts.get(tableName);
            ChangeDataTableCount rawCount = rawZoneCounts.get(tableName);
            sb.append("For table ").append(tableName);
            if (dmsCount != null && dmsCount.equals(dmsAppliedCount) && dmsCount.equals(rawCount)) {
                sb.append(" MATCH:\n");
            } else {
                sb.append(" DOES NOT MATCH:\n");
            }

            sb.append("\t")
                    .append(nullSafeCountString(rawCount))
                    .append("\t")
                    .append(" - Raw")
                    .append("\n");
            sb.append("\t")
                    .append(nullSafeCountString(dmsCount))
                    .append("\t")
                    .append(" - DMS")
                    .append("\n");
            sb.append("\t")
                    .append(nullSafeCountString(dmsAppliedCount))
                    .append("\t")
                    .append(" - DMS Applied")
                    .append("\n");
        });

        return sb.toString();
    }

    private static String sortedListOfTables(Set<String> tables) {
        return tables.stream().sorted().collect(Collectors.joining(", "));
    }

    private boolean rawCountsMatchDmsCounts() {
        return rawZoneCounts.entrySet().stream().allMatch(entry -> {
            String tableName = entry.getKey();
            ChangeDataTableCount rawCount = entry.getValue();
            ChangeDataTableCount dmsCount = dmsCounts.get(tableName);
            ChangeDataTableCount dmsAppliedCount = dmsAppliedCounts.get(tableName);
            return rawCount.equals(dmsCount) && dmsCount.equals(dmsAppliedCount);
        });
    }

    private boolean sameTables() {
        return rawZoneCounts.keySet().equals(dmsCounts.keySet()) && rawZoneCounts.keySet().equals(dmsAppliedCounts.keySet());
    }

    private String nullSafeCountString(ChangeDataTableCount count) {
        if (count != null) {
            return count.toString();
        } else {
            return MISSING_COUNTS_MESSAGE;
        }
    }
}
