CREATE EXTERNAL TABLE curated.nomis_offender_external_movements
LOCATION 's3://dpr-curated-zone-test/nomis/offender_external_movements/'
TBLPROPERTIES (
'table_type'='DELTA'
);