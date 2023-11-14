package uk.gov.justice.digital.job.cdc;

import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class TableDiscovery {

    List<ImmutablePair<String, String>> discoverTablesToProcess() {
        // TODO Discover the tables from S3 or fix the broken SourceReferenceService
        List<ImmutablePair<String, String>> tablesToProcess = new ArrayList<>();
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "AGENCY_INTERNAL_LOCATIONS"));
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "AGENCY_LOCATIONS"));
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "MOVEMENT_REASONS"));
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "OFFENDER_BOOKINGS"));
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "OFFENDER_EXTERNAL_MOVEMENTS"));
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "OFFENDERS"));
        return tablesToProcess;
    }
}
