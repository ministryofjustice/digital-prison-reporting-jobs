package uk.gov.justice.digital.domains.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TableDefinition {

    private String name;
    private String description;
    private String version;
    private String location;
    private String primaryKey;

    private Map<String,String> tags = new HashMap<>();

    // permissions

    private String owner;
    private String author;

    private TransformDefinition transform;
    private MappingDefinition mapping;
    private List<ViolationDefinition> violations = new ArrayList<>();

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public TransformDefinition getTransform() {
        return transform;
    }

    public void setTransform(TransformDefinition transform) {
        this.transform = transform;
    }

    public MappingDefinition getMapping() {
        return mapping;
    }

    public void setMapping(MappingDefinition mapping) {
        this.mapping = mapping;
    }

    public List<ViolationDefinition> getViolations() {
        return violations;
    }

    public void setViolations(List<ViolationDefinition> violations) {
        this.violations = violations;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TransformDefinition {
        private String viewText;
        private List<String> sources = new ArrayList<String>();

        public String getViewText() {
            return viewText;
        }
        public void setViewText(String viewText) {
            this.viewText = viewText;
        }
        public List<String> getSources() {
            return sources;
        }
        public void setSources(List<String> sources) {
            this.sources = sources;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MappingDefinition {
        private String viewText;

        public String getViewText() {
            return viewText;
        }
        public void setViewText(String viewText) {
            this.viewText = viewText;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ViolationDefinition {
        private String check;
        private String location;
        private String name;

        public String getCheck() {
            return check;
        }
        public void setCheck(String check) {
            this.check = check;
        }
        public String getLocation() {
            return location;
        }
        public void setLocation(String location) {
            this.location = location;
        }
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }

    }

}

