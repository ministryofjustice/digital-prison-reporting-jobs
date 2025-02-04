package uk.gov.justice.digital.datahub.model;

import java.time.Clock;
import java.time.LocalDateTime;

public class FileLastModifiedDate {

    public final String key;
    public final LocalDateTime lastModifiedDateTime;

    public FileLastModifiedDate(String key, LocalDateTime lastModifiedDateTime) {
        this.key = key;
        this.lastModifiedDateTime = lastModifiedDateTime;
    }

    public FileLastModifiedDate(String key) {
        this.key = key;
        this.lastModifiedDateTime = LocalDateTime.now(Clock.systemUTC());
    }
}
