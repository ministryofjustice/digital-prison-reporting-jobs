package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
        return sameTables() && rawCountsMatchDmsCountsWithinTolerance();
    }

    @Override
    public String summary() {
        StringBuilder sb = new StringBuilder("Change Data Total Counts ");
        if (sameTables() && rawCountsExactMatchDmsCounts()) {
            sb.append("MATCH:\n\n");
        } else if (isSuccess()) {
            sb.append("MATCH (within tolerance):\n\n");
        } else {
            sb.append("DO NOT MATCH:\n\n");
        }

        if (!sameTables()) {
            sb.append("\nThe set of tables for DMS vs Raw zone DO NOT MATCH\n\n");
            sb.append("DMS Tables missing in Raw: ").append(tablesInDmsMissingFromRaw()).append("\n");
            sb.append("Raw Zone/Raw Archive Tables missing in DMS: ").append(tablesInRawMissingFromDms()).append("\n");
        }

        dmsCounts.forEach((tableName, dmsCount) -> {
            ChangeDataTableCount dmsAppliedCount = dmsAppliedCounts.get(tableName);
            ChangeDataTableCount rawCount = rawZoneCounts.get(tableName);
            sb.append("For table ").append(tableName);
            if (dmsCount != null && dmsCount.equals(dmsAppliedCount) && dmsCount.equals(rawCount)) {
                sb.append(" MATCH:\n");
            } else if (dmsCount != null && dmsCount.countsEqualWithinTolerance(dmsAppliedCount) && dmsCount.countsEqualWithinTolerance(rawCount)) {
                sb.append(" MATCH (within tolerance):\n");
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

    private boolean rawCountsMatchDmsCountsWithinTolerance() {
        return rawZoneCounts.entrySet().stream().allMatch(entry -> {
            String tableName = entry.getKey();
            ChangeDataTableCount rawCount = entry.getValue();
            ChangeDataTableCount dmsCount = dmsCounts.get(tableName);
            ChangeDataTableCount dmsAppliedCount = dmsAppliedCounts.get(tableName);
            return rawCount.countsEqualWithinTolerance(dmsCount) && dmsCount.countsEqualWithinTolerance(dmsAppliedCount);
        });
    }

    private boolean rawCountsExactMatchDmsCounts() {
        return rawZoneCounts.entrySet().stream().allMatch(entry -> {
            String tableName = entry.getKey();
            ChangeDataTableCount rawCount = entry.getValue();
            ChangeDataTableCount dmsCount = dmsCounts.get(tableName);
            ChangeDataTableCount dmsAppliedCount = dmsAppliedCounts.get(tableName);
            return rawCount.countsEqual(dmsCount) && dmsCount.countsEqual(dmsAppliedCount);
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

    private Set<String> tablesInDmsMissingFromRaw() {
        Set<String> result = new HashSet<>(dmsCounts.keySet());
        result.removeAll(rawZoneCounts.keySet());
        return result;
    }

    private Set<String> tablesInRawMissingFromDms() {
        HashSet<String> result = new HashSet<>(rawZoneCounts.keySet());
        result.removeAll(dmsCounts.keySet());
        return result;
    }
}
