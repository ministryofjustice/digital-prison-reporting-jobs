CREATE EXTERNAL TABLE dms_structured.nomis_offender_external_movements
LOCATION 's3://dpr-dms-structured-zone-development/nomis/offender_external_movements/'
TBLPROPERTIES (
'table_type'='DELTA'
);