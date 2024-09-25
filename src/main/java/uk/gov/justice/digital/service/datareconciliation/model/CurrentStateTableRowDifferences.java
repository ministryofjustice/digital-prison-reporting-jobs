package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Data;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Represents the results of running the row differences reconciliation for the "current state" data in DataHub
 * for a single table.
 */
@Data
public class CurrentStateTableRowDifferences {
    private final Row[] inSourceNotCuratedPks;
    private final Row[] inCuratedNotSourcePks;


    public boolean rowsMatch() {
        return inSourceNotCuratedPks.length == 0 && inCuratedNotSourcePks.length == 0;
    }

    public String summary() {
        return "In Data Source but not Curated: " + inSourceNotCuratedPks.length +
                ", in Curated but not Data Source: " + inCuratedNotSourcePks.length +
                (rowsMatch() ? "\t - MATCH" : "\t - MISMATCH");
    }

    public String details() {
        return "In Data Source but not Curated:\n" + rowArrayToPkPerLine(inSourceNotCuratedPks) +
                "In Curated but not Data Source:\n" + rowArrayToPkPerLine(inCuratedNotSourcePks);
    }

    private String rowArrayToPkPerLine(Row[] rows) {
        return Arrays.stream(rows).map(Row::toString).collect(Collectors.joining("\n"));
    }
}
