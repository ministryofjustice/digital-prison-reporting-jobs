CREATE EXTERNAL TABLE dms_curated.nomis_offender_bookings
LOCATION 's3://dpr-dms-curated-zone-development/nomis/offender_bookings/'
TBLPROPERTIES (
'table_type'='DELTA'
);