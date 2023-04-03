package uk.gov.justice.digital.domains.model;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DomainRepoRecord {

    public static StructType SCHEMA = new StructType()
            .add("name", DataTypes.StringType)
            .add("version", DataTypes.StringType)
            .add("active", DataTypes.BooleanType)
            .add("location", DataTypes.StringType)
            .add("sources", new ArrayType(DataTypes.StringType, true))
            .add("definition", DataTypes.StringType);

    protected String name;
    protected String version;
    protected boolean active;
    protected String location;
    protected Set<String> sources = new HashSet<String>();
    protected String definition;

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getVersion() {
        return version;
    }
    public void setVersion(String version) {
        this.version = version;
    }
    public boolean isActive() {
        return active;
    }
    public void setActive(boolean active) {
        this.active = active;
    }
    public String getLocation() {
        return location;
    }
    public void setLocation(String location) {
        this.location = location;
    }
    public Set<String> getSources() {
        return sources;
    }
    public void setSources(Set<String> sources) {
        this.sources = sources;
    }
    public String getDefinition() {
        return definition;
    }
    public void setDefinition(String definition) {
        this.definition = definition;
    }

    public Row toRow() {
        Seq<?> sources = JavaConverters.asScalaIteratorConverter(this.sources.iterator()).asScala().toSeq();
        List<Object> items = Arrays.asList(new Object[] { this.name, this.version, this.active, this.location, sources, this.definition});
        return Row.fromSeq(JavaConverters.asScalaIteratorConverter(items.iterator()).asScala().toSeq());
    }

}
