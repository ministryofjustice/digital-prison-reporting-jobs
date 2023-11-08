CREATE EXTERNAL TABLE `dms_raw`.`oms_owner_agency_locations`
(
    `Op`                            string COMMENT '',
    `_timestamp`                    string COMMENT '',
    `AGY_LOC_ID`                    string COMMENT '',
    `DESCRIPTION`                   string COMMENT '',
    `AGENCY_LOCATION_TYPE`          string COMMENT '',
    `DISTRICT_CODE`                 string COMMENT '',
    `UPDATED_ALLOWED_FLAG`          string COMMENT '',
    `ABBREVIATION`                  string COMMENT '',
    `DEACTIVATION_DATE`             timestamp COMMENT '',
    `CONTACT_NAME`                  string COMMENT '',
    `PRINT_QUEUE`                   string COMMENT '',
    `JURISDICTION_CODE`             string COMMENT '',
    `BAIL_OFFICE_FLAG`              string COMMENT '',
    `LIST_SEQ`                      integer COMMENT '',
    `HOUSING_LEV_1_CODE`            string COMMENT '',
    `HOUSING_LEV_2_CODE`            string COMMENT '',
    `HOUSING_LEV_3_CODE`            string COMMENT '',
    `HOUSING_LEV_4_CODE`            string COMMENT '',
    `PROPERTY_LEV_1_CODE`           string COMMENT '',
    `PROPERTY_LEV_2_CODE`           string COMMENT '',
    `PROPERTY_LEV_3_CODE`           string COMMENT '',
    `LAST_BOOKING_NO`               decimal(10, 0) COMMENT '',
    `COMMISSARY_PRIVILEGE`          string COMMENT '',
    `BUSINESS_HOURS`                string COMMENT '',
    `ADDRESS_TYPE`                  string COMMENT '',
    `SERVICE_REQUIRED_FLAG`         string COMMENT '',
    `ACTIVE_FLAG`                   string COMMENT '',
    `DISABILITY_ACCESS_CODE`        string COMMENT '',
    `INTAKE_FLAG`                   string COMMENT '',
    `SUB_AREA_CODE`                 string COMMENT '',
    `AREA_CODE`                     string COMMENT '',
    `NOMS_REGION_CODE`              string COMMENT '',
    `GEOGRAPHIC_REGION_CODE`        string COMMENT '',
    `JUSTICE_AREA_CODE`             string COMMENT '',
    `CJIT_CODE`                     string COMMENT '',
    `LONG_DESCRIPTION`              string COMMENT '',
    `CREATE_DATETIME`               timestamp COMMENT '',
    `CREATE_USER_ID`                string COMMENT '',
    `MODIFY_DATETIME`               timestamp COMMENT '',
    `MODIFY_USER_ID`                string COMMENT '',
    `AUDIT_TIMESTAMP`               timestamp COMMENT '',
    `AUDIT_USER_ID`                 string COMMENT '',
    `AUDIT_MODULE_NAME`             string COMMENT '',
    `AUDIT_CLIENT_USER_ID`          string COMMENT '',
    `AUDIT_CLIENT_IP_ADDRESS`       string COMMENT '',
    `AUDIT_CLIENT_WORKSTATION_NAME` string COMMENT '',
    `AUDIT_ADDITIONAL_INFO`         string COMMENT '',
    `PAYROLL_REGION`                string COMMENT ''
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://dpr-dms-raw-zone-development/OMS_OWNER/AGENCY_LOCATIONS/'
    TBLPROPERTIES (
        'classification' = 'parquet')