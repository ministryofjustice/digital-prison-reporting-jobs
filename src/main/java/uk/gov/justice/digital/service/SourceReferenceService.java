package uk.gov.justice.digital.service;

import lombok.val;
import org.apache.spark.sql.types.DataType;
import uk.gov.justice.digital.service.model.SourceReference;

import java.io.InputStream;
import java.util.*;

/**
 * Temporary Internal Schema storage using json schema files.
 *
 * This will be replaced by the Hive Metastore in later work.
 *
 * See DPR-246 for further details.
 */
public class SourceReferenceService {

    private static final Map<String, SourceReference> sources = new HashMap<>();

    static {
        sources.put("oms_owner.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders", "OFFENDER_ID", getSchemaFromResource("/schemas/oms_owner.offenders.schema.json")));
        sources.put("oms_owner.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "nomis", "offender_bookings", "OFFENDER_BOOK_ID", getSchemaFromResource("/schemas/oms_owner.offender_bookings.schema.json")));
        sources.put("oms_owner.agency_locations", new SourceReference("SYSTEM.AGENCY_LOCATIONS", "nomis", "agency_locations", "AGY_LOC_ID", getSchemaFromResource("/schemas/oms_owner.agency_locations.schema.json")));
        sources.put("oms_owner.agency_internal_locations", new SourceReference("SYSTEM.AGENCY_INTERNAL_LOCATIONS", "nomis", "agency_internal_locations", "INTERNAL_LOCATION_ID", getSchemaFromResource("/schemas/oms_owner.agency_internal_locations.schema.json")));

        sources.put("public.offenders", new SourceReference("SYSTEM.OFFENDERS", "public", "offenders", "OFFENDER_ID", getSchemaFromResource("/schemas/oms_owner.offenders.schema.json")));
        sources.put("public.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "public", "offender_bookings", "OFFENDER_BOOK_ID", getSchemaFromResource("/schemas/oms_owner.offender_bookings.schema.json")));
    }

    // TODO - refactor these
    //      - any value in returning Optionals?
    public static String getPrimaryKey(final String key) {
        val ref = sources.get(key.toLowerCase());
        return (ref == null ? null : ref.getPrimaryKey());
    }

    public static String getSource(String table, String source) {
        val ref = sources.get(generateKey(table, source));
        return (ref == null ? null : ref.getSource());
    }

    public static String getTable(String table, String source) {
        val ref = sources.get(generateKey(table, source));
        return (ref == null ? null : ref.getTable());
    }
    public static DataType getSchema(String table, String source) {
        val ref = sources.get(generateKey(table, source));
        return (ref == null ? null : ref.getSchema());
    }

    private static DataType getSchemaFromResource(final String resource) {
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

    private static String generateKey(String table, String source) {
        return String.join(".", table, source).toLowerCase();
    }

    public Set<SourceReference> getReferences() {
        return new HashSet<>(sources.values());
    }

}
