package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Data;

import static java.lang.String.format;

@Data
public class PrimaryKeyReconciliationCount {

    private final long countInCuratedNotDataSource;
    private final long countInDataSourceNotCurated;

    public boolean countsAreZero() {
        return countInCuratedNotDataSource == 0 && countInDataSourceNotCurated == 0;
    }

    @Override
    public String toString() {
        return format(
                "In Curated but not Data Source: %d, In Data Source but not Curated: %d",
                countInCuratedNotDataSource,
                countInDataSourceNotCurated
        );
    }

}
