CREATE EXTERNAL TABLE structured.nomis_offender_bookings
LOCATION 's3://dpr-structured-zone-test/nomis/offender_bookings/'
TBLPROPERTIES (
'table_type'='DELTA'
);