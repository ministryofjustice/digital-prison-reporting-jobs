CREATE EXTERNAL TABLE curated.nomis_offender_bookings
LOCATION 's3://dpr-curated-zone-test/nomis/offender_bookings/'
TBLPROPERTIES (
'table_type'='DELTA'
);