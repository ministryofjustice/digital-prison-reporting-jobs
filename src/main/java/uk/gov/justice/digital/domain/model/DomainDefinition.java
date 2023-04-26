package uk.gov.justice.digital.domain.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class DomainDefinition {
    private String id;
    private String name;
    private String description;
    private String version;
    private String location;

    private Map<String,String> tags;

    // permissions

    private String owner;
    private String author;

    private List<TableDefinition> tables = new ArrayList<>();

}
