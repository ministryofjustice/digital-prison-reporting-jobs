SELECT
    (raw_total - structured_total) = 0
        AND
    (raw_total - curated_total) = 0
    As passed,
    *
FROM
    (
    SELECT
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_agency_internal_locations"
             where Op = 'I')
            -
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_agency_internal_locations"
             where Op = 'D')
            As raw_total,

            (SELECT count(*)
             FROM "dms_structured"."nomis_agency_internal_locations")
            As structured_total,

            (SELECT count(*)
             FROM "dms_curated"."nomis_agency_internal_locations")
            As curated_total,

        'agency_internal_locations' as tbl
    UNION ALL
    SELECT
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_agency_locations"
             where Op = 'I')
            -
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_agency_locations"
             where Op = 'D')
                                        As raw_total,

            (SELECT count(*)
             FROM "dms_structured"."nomis_agency_locations")
                                        As structured_total,

            (SELECT count(*)
             FROM "dms_curated"."nomis_agency_locations")
                                        As curated_total,

            'agency_locations' as tbl
    UNION ALL
    SELECT
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_movement_reasons"
             where Op = 'I')
            -
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_movement_reasons"
             where Op = 'D')
                                        As raw_total,

            (SELECT count(*)
             FROM "dms_structured"."nomis_movement_reasons")
                                        As structured_total,

            (SELECT count(*)
             FROM "dms_curated"."nomis_movement_reasons")
                                        As curated_total,

            'movement_reasons' as tbl
    UNION ALL
    SELECT
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_offender_bookings"
             where Op = 'I')
            -
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_offender_bookings"
             where Op = 'D')
                                        As raw_total,

            (SELECT count(*)
             FROM "dms_structured"."nomis_offender_bookings")
                                        As structured_total,

            (SELECT count(*)
             FROM "dms_curated"."nomis_offender_bookings")
                                        As curated_total,

            'offender_bookings' as tbl
    UNION ALL
    SELECT
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_offender_external_movements"
             where Op = 'I')
            -
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_offender_external_movements"
             where Op = 'D')
                                        As raw_total,

            (SELECT count(*)
             FROM "dms_structured"."nomis_offender_external_movements")
                                        As structured_total,

            (SELECT count(*)
             FROM "dms_curated"."nomis_offender_external_movements")
                                        As curated_total,

            'offender_external_movements' as tbl
    UNION ALL
    SELECT
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_offenders"
             where Op = 'I')
            -
            (SELECT count(*)
             FROM "dms_raw"."oms_owner_offenders"
             where Op = 'D')
                                        As raw_total,

            (SELECT count(*)
             FROM "dms_structured"."nomis_offenders")
                                        As structured_total,

            (SELECT count(*)
             FROM "dms_curated"."nomis_offenders")
                                        As curated_total,

            'offenders' as tbl
    )
ORDER BY tbl DESC