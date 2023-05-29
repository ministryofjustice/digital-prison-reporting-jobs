package uk.gov.justice.digital.service;

import org.apache.spark.sql.types.StructType;
import uk.gov.justice.digital.domain.model.SourceReference;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;

/**
 * Temporary Internal Schema storage using json schema files.
 * <p>
 * This will be replaced by the Hive Metastore in later work.
 * <p>
 * See DPR-246 for further details.
 */
public class SourceReferenceService {

    private SourceReferenceService() {}

    private static final Map<String, SourceReference> sources = new HashMap<>();

    static {
        sources.put("oms_owner.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders", "OFFENDER_ID", getSchemaFromResource("/schemas/oms_owner.offenders.schema.json")));
        sources.put("oms_owner.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "nomis", "offender_bookings", "OFFENDER_BOOK_ID", getSchemaFromResource("/schemas/oms_owner.offender_bookings.schema.json")));
        sources.put("oms_owner.agency_locations", new SourceReference("SYSTEM.AGENCY_LOCATIONS", "nomis", "agency_locations", "AGY_LOC_ID", getSchemaFromResource("/schemas/oms_owner.agency_locations.schema.json")));
        sources.put("oms_owner.agency_internal_locations", new SourceReference("SYSTEM.AGENCY_INTERNAL_LOCATIONS", "nomis", "agency_internal_locations", "INTERNAL_LOCATION_ID", getSchemaFromResource("/schemas/oms_owner.agency_internal_locations.schema.json")));
    }

    public static Optional<SourceReference> getSourceReference(String source, String table) {
        return Optional.ofNullable(sources.get(generateKey(source, table)));
    }

    private static StructType getSchemaFromResource(String resource) {
        return (StructType) Optional.ofNullable(System.class.getResourceAsStream(resource))
                .map(SourceReferenceService::readInputStream)
                .map(StructType::fromJson)
                .orElseThrow(() -> new IllegalStateException("Failed to read resource: " + resource));
    }

    private static String readInputStream(InputStream is) {
        return new Scanner(is, "UTF-8")
                .useDelimiter("\\A") // Matches the next boundary which in our case will be EOF.
                .next();
    }

    public static String generateKey(String source, String table) {
        return String.join(".", source, table).toLowerCase();
    }

}
