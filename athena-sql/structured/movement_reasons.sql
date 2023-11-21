CREATE EXTERNAL TABLE structured.nomis_movement_reasons
LOCATION 's3://dpr-structured-zone-test/nomis/movement_reasons/'
TBLPROPERTIES (
'table_type'='DELTA'
);