CREATE EXTERNAL TABLE curated.nomis_movement_reasons
LOCATION 's3://dpr-curated-zone-test/nomis/movement_reasons/'
TBLPROPERTIES (
'table_type'='DELTA'
);