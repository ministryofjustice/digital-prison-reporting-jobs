CREATE EXTERNAL TABLE structured.nomis_offender_external_movements
LOCATION 's3://dpr-structured-zone-test/nomis/offender_external_movements/'
TBLPROPERTIES (
'table_type'='DELTA'
);