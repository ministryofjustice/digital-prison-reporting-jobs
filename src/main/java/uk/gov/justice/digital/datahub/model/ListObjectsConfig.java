package uk.gov.justice.digital.datahub.model;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;

public class ListObjectsConfig {

    public enum ObjectModifiedDateTime {
        EARLIER,
        LATER,
        ON_OR_EARLIER,
        ON_OR_LATER
    }

    public final Duration period;
    public final ObjectModifiedDateTime objectModifiedDateTime;
    public final Clock clock;

    public ListObjectsConfig(ObjectModifiedDateTime objectModifiedDateTime, Duration period, Clock clock) {
        this.objectModifiedDateTime = objectModifiedDateTime;
        this.period = period;
        this.clock = clock;
    }

    public boolean isWithinPeriod(LocalDateTime modifiedDateTime) {
        LocalDateTime currentDateTime = LocalDateTime.now(clock);
        switch (this.objectModifiedDateTime) {
            case EARLIER:
                return modifiedDateTime.isBefore(currentDateTime.minus(period));
            case LATER:
                return modifiedDateTime.isAfter(currentDateTime.minus(period));
            case ON_OR_LATER:
                return modifiedDateTime.isEqual(currentDateTime.minus(period)) || modifiedDateTime.isAfter(currentDateTime.minus(period));
            case ON_OR_EARLIER:
                return modifiedDateTime.isEqual(currentDateTime.minus(period)) || modifiedDateTime.isBefore(currentDateTime.minus(period));
        }
        return false;
    }

    public boolean exitWhenOutsidePeriod() {
        return this.objectModifiedDateTime == ObjectModifiedDateTime.LATER || this.objectModifiedDateTime == ObjectModifiedDateTime.ON_OR_LATER;
    }
}
