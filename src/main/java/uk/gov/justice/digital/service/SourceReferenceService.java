package uk.gov.justice.digital.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.shaded.com.nimbusds.jose.util.IOUtils;
import org.apache.spark.sql.types.DataType;

import com.google.common.base.Charsets;

public class SourceReferenceService {

    private static final Map<String, SourceReference> REF = new HashMap<String,SourceReference>();

    /**
     * For the PoC we will hardcode the references - although we would like there to be something
     * proper in place in future, as we onboard each service.
     */
    static {
        // demo
        try {
            REF.put("system.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders", "OFFENDER_ID", getSchemaFromResource("/schemas/oms_owner.offenders.schema.json")));
            REF.put("system.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "nomis", "offender_bookings", "OFFENDER_BOOK_ID", getSchemaFromResource("/schemas/oms_owner.offender_bookings.schema.json")));
            REF.put("system.agency_locations", new SourceReference("SYSTEM.AGENCY_LOCATIONS", "nomis", "agency_locations", "AGY_LOC_ID", getSchemaFromResource("/schemas/oms_owner.agency_locations.schema.json")));
            REF.put("system.agency_internal_locations", new SourceReference("SYSTEM.AGENCY_INTERNAL_LOCATIONS", "nomis", "agency_internal_locations", "INTERNAL_LOCATION_ID", getSchemaFromResource("/schemas/oms_owner.agency_internal_locations.schema.json")));

            // t3
            REF.put("oms_owner.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders", "OFFENDER_ID", getSchemaFromResource("/schemas/oms_owner.offenders.schema.json")));
            REF.put("oms_owner.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "nomis", "offender_bookings", "OFFENDER_BOOK_ID", getSchemaFromResource("/schemas/oms_owner.offender_bookings.schema.json")));
            REF.put("oms_owner.agency_locations", new SourceReference("SYSTEM.AGENCY_LOCATIONS", "nomis", "agency_locations", "AGY_LOC_ID", getSchemaFromResource("/schemas/oms_owner.agency_locations.schema.json")));
            REF.put("oms_owner.agency_internal_locations", new SourceReference("SYSTEM.AGENCY_INTERNAL_LOCATIONS", "nomis", "agency_internal_locations", "INTERNAL_LOCATION_ID", getSchemaFromResource("/schemas/oms_owner.agency_internal_locations.schema.json")));


            REF.put("public.statement", new SourceReference("public.statement", "use_of_force", "statement", "id", getSchemaFromResource("/schemas/use-of-force.statement.schema.json")));
            REF.put("public.report", new SourceReference("public.report", "use_of_force", "report", "id", getSchemaFromResource("/schemas/use-of-force.report.schema.json")));
        } catch (IOException e) {
            handleError(e);
        }
    }

    public static String getPrimaryKey(final String key) {
        final SourceReference ref = REF.get(key.toLowerCase());
        return (ref == null ? null : ref.getPrimaryKey());
    }

    public static String getSource(final String key) {
        final SourceReference ref = REF.get(key.toLowerCase());
        return (ref == null ? null : ref.getSource());
    }

    public static String getTable(final String key) {
        final SourceReference ref = REF.get(key.toLowerCase());
        return (ref == null ? null : ref.getTable());
    }
    public static DataType getSchema(final String key) {
        final SourceReference ref = REF.get(key.toLowerCase());
        return (ref == null ? null : ref.getSchema());
    }

    protected static DataType getSchemaFromResource(final String resource) throws IOException {
        final InputStream stream = getStream(resource);
        if(stream != null) {
            return DataType.fromJson(IOUtils.readInputStreamToString(stream, Charsets.UTF_8));
        }
        return null;
    }

    public static Map<String,String> getCasts(final String key) {
        final SourceReference ref = REF.get(key.toLowerCase());
        return (ref == null ? null : ref.getCasts());
    }

    public static Set<SourceReference> getReferences() {
        return new HashSet<SourceReference>(REF.values());
    }

    protected static InputStream getStream(final String resource) {
        InputStream stream = System.class.getResourceAsStream(resource);
        if(stream == null) {
            stream = System.class.getResourceAsStream("/src/main/resources" + resource);
            if(stream == null) {
                stream = System.class.getResourceAsStream("/target/classes" + resource);
                if(stream == null) {
                    Path root = Paths.get(".").normalize().toAbsolutePath();
                    stream = System.class.getResourceAsStream(root.toString() + "/src/main/resources" + resource);
                    if(stream == null) {
                        stream = SourceReferenceService.class.getResourceAsStream(resource);
                    }
                }
            }
        }
        return stream;
    }

    protected static void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        System.err.print(sw.getBuffer().toString());
    }

    public static class SourceReference {

        private String key;
        private String source;
        private String table;
        private String primaryKey;
        private DataType schema;
        private Map<String,String> casts;

        public SourceReference(final String key, final String source, final String table, final String primaryKey) {
            this.key = key;
            this.source = source;
            this.table = table;
            this.primaryKey = primaryKey;
            this.casts = new HashMap<String,String>();
        }

        public SourceReference(final String key, final String source, final String table, final String primaryKey, DataType schema) {
            this.key = key;
            this.source = source;
            this.table = table;
            this.primaryKey = primaryKey;
            this.schema = schema;
            this.casts = new HashMap<String,String>();
        }

        public SourceReference(final String key, final String source, final String table, final String primaryKey, Map<String,String> casts) {
            this.key = key;
            this.source = source;
            this.table = table;
            this.primaryKey = primaryKey;
            this.casts = casts;
        }

        public String getKey() {
            return key;
        }
        public void setKey(String key) {
            this.key = key;
        }
        public String getSource() {
            return source;
        }
        public void setSource(String source) {
            this.source = source;
        }
        public String getTable() {
            return table;
        }
        public void setTable(String table) {
            this.table = table;
        }
        public String getPrimaryKey() {
            return primaryKey;
        }
        public void setPrimaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
        }
        public DataType getSchema() {
            return schema;
        }
        public void setSchema(DataType schema) {
            this.schema = schema;
        }
        public Map<String, String> getCasts() {
            return casts;
        }
        public void setCasts(Map<String, String> casts) {
            this.casts = casts;
        }
    }
}
