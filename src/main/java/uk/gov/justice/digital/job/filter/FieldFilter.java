package uk.gov.justice.digital.job.filter;

import java.util.Map.Entry;

public interface FieldFilter {

    boolean isEligible(String fieldName);

    Entry<String, Object> apply(Entry<String, Object> entry);

    default Entry<String, Object> applyIfEligible(Entry<String, Object> entry) {
        return (isEligible(entry.getKey()))
                ? apply(entry)
                : entry;
    }

}
