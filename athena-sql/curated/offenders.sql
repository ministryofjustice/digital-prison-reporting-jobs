CREATE EXTERNAL TABLE curated.nomis_offenders
LOCATION 's3://dpr-curated-zone-test/nomis/offenders/'
TBLPROPERTIES (
'table_type'='DELTA'
);