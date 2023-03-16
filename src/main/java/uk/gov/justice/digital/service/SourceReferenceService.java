package uk.gov.justice.digital.service;

import lombok.val;
import org.apache.spark.sql.types.DataType;
import uk.gov.justice.digital.service.model.SourceReference;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public class SourceReferenceService {

    private static final Map<String, SourceReference> sources = new HashMap<String,SourceReference>();

    // Temporarily hardcoded until we extract this into s3/Dynamo/???
    static {
        try {
            sources.put("oms_owner.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders", "OFFENDER_ID", getSchemaFromResource("/schemas/oms_owner.offenders.schema.json")));
            sources.put("oms_owner.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "nomis", "offender_bookings", "OFFENDER_BOOK_ID", getSchemaFromResource("/schemas/oms_owner.offender_bookings.schema.json")));
            sources.put("oms_owner.agency_locations", new SourceReference("SYSTEM.AGENCY_LOCATIONS", "nomis", "agency_locations", "AGY_LOC_ID", getSchemaFromResource("/schemas/oms_owner.agency_locations.schema.json")));
            sources.put("oms_owner.agency_internal_locations", new SourceReference("SYSTEM.AGENCY_INTERNAL_LOCATIONS", "nomis", "agency_internal_locations", "INTERNAL_LOCATION_ID", getSchemaFromResource("/schemas/oms_owner.agency_internal_locations.schema.json")));

            sources.put("public.offenders", new SourceReference("SYSTEM.OFFENDERS", "public", "offenders", "OFFENDER_ID", getSchemaFromResource("/schemas/oms_owner.offenders.schema.json")));
            sources.put("public.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "public", "offender_bookings", "OFFENDER_BOOK_ID", getSchemaFromResource("/schemas/oms_owner.offender_bookings.schema.json")));
        } catch (IOException e) {
            handleError(e);
        }
    }

    public static String getPrimaryKey(final String key) {
        final SourceReference ref = sources.get(key.toLowerCase());
        return (ref == null ? null : ref.getPrimaryKey());
    }

    public static String getSource(final String key) {
        final SourceReference ref = sources.get(key.toLowerCase());
        return (ref == null ? null : ref.getSource());
    }

    public static String getTable(final String key) {
        final SourceReference ref = sources.get(key.toLowerCase());
        return (ref == null ? null : ref.getTable());
    }
    public static DataType getSchema(final String key) {
        final SourceReference ref = sources.get(key.toLowerCase());
        return (ref == null ? null : ref.getSchema());
    }

    // TODO - review usages of this
    protected static DataType getSchemaFromResource(final String resource) throws IOException {
        return Optional.ofNullable(System.class.getResourceAsStream(resource))
            .map(SourceReferenceService::readInputStream)
            .map(DataType::fromJson)
            .orElseThrow(() -> new IllegalStateException("Failed to read resource: " + resource));
    }

    private static String readInputStream(InputStream is) {
        return new Scanner(is, "UTF-8")
            .useDelimiter("\\A") // Matches the next boundary which in our case will be EOF.
            .next();
    }

    public static Set<SourceReference> getReferences() {
        return new HashSet<SourceReference>(sources.values());
    }

    protected static void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        System.err.print(sw.getBuffer().toString());
    }

}
