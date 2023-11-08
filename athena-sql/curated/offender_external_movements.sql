CREATE EXTERNAL TABLE dms_curated.nomis_offender_external_movements
LOCATION 's3://dpr-dms-curated-zone-development/nomis/offender_external_movements/'
TBLPROPERTIES (
'table_type'='DELTA'
);