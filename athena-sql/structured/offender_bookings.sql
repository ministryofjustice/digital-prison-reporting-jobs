CREATE EXTERNAL TABLE dms_structured.nomis_offender_bookings
LOCATION 's3://dpr-dms-structured-zone-development/nomis/offender_bookings/'
TBLPROPERTIES (
'table_type'='DELTA'
);