SELECT * FROM
    (SELECT 'agency_internal_locations' as tbl,
            Op,
            count(*)                    as cnt
     FROM "raw"."nomis_agency_internal_locations"
     GROUP BY Op
     UNION ALL
     SELECT
         'agency_locations' as tbl, Op, count(*) as cnt
     FROM "raw"."nomis_agency_locations"
     GROUP BY Op
     UNION ALL
     SELECT
         'movement_reasons' as tbl, Op, count(*) as cnt
     FROM "raw"."nomis_movement_reasons"
     GROUP BY Op
     UNION ALL
     SELECT
         'offender_bookings' as tbl, Op, count(*) as cnt
     FROM "raw"."nomis_offender_bookings"
     GROUP BY Op
     UNION ALL
     SELECT
         'offender_external_movements' as tbl, Op, count(*) as cnt
     FROM "raw"."nomis_offender_external_movements"
     GROUP BY Op
     UNION ALL
     SELECT
         'offenders' as tbl, Op, count(*) as cnt
     FROM "raw"."nomis_offenders"
     GROUP BY Op)
ORDER BY tbl DESC, Op ASC