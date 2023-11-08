CREATE EXTERNAL TABLE dms_structured.nomis_movement_reasons
LOCATION 's3://dpr-dms-structured-zone-development/nomis/movement_reasons/'
TBLPROPERTIES (
'table_type'='DELTA'
);