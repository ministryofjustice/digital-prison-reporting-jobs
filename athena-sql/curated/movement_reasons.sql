CREATE EXTERNAL TABLE dms_curated.nomis_movement_reasons
LOCATION 's3://dpr-dms-curated-zone-development/nomis/movement_reasons/'
TBLPROPERTIES (
'table_type'='DELTA'
);