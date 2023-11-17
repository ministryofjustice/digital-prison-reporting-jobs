CREATE EXTERNAL TABLE `dms_raw`.`oms_owner_offenders`
(
    `Op`                            string COMMENT '',
    `_timestamp`                    string COMMENT '',
    `OFFENDER_ID`                   decimal(10, 0) COMMENT '',
    `OFFENDER_NAME_SEQ`             decimal(38, 0) COMMENT '',
    `ID_SOURCE_CODE`                string COMMENT '',
    `LAST_NAME`                     string COMMENT '',
    `NAME_TYPE`                     string COMMENT '',
    `FIRST_NAME`                    string COMMENT '',
    `MIDDLE_NAME`                   string COMMENT '',
    `BIRTH_DATE`                    timestamp COMMENT '',
    `SEX_CODE`                      string COMMENT '',
    `SUFFIX`                        string COMMENT '',
    `LAST_NAME_SOUNDEX`             string COMMENT '',
    `BIRTH_PLACE`                   string COMMENT '',
    `BIRTH_COUNTRY_CODE`            string COMMENT '',
    `CREATE_DATE`                   timestamp COMMENT '',
    `LAST_NAME_KEY`                 string COMMENT '',
    `ALIAS_OFFENDER_ID`             decimal(10, 0) COMMENT '',
    `FIRST_NAME_KEY`                string COMMENT '',
    `MIDDLE_NAME_KEY`               string COMMENT '',
    `OFFENDER_ID_DISPLAY`           string COMMENT '',
    `ROOT_OFFENDER_ID`              decimal(10, 0) COMMENT '',
    `CASELOAD_TYPE`                 string COMMENT '',
    `MODIFY_USER_ID`                string COMMENT '',
    `MODIFY_DATETIME`               timestamp COMMENT '',
    `ALIAS_NAME_TYPE`               string COMMENT '',
    `PARENT_OFFENDER_ID`            decimal(10, 0) COMMENT '',
    `UNIQUE_OBLIGATION_FLAG`        string COMMENT '',
    `SUSPENDED_FLAG`                string COMMENT '',
    `SUSPENDED_DATE`                timestamp COMMENT '',
    `RACE_CODE`                     string COMMENT '',
    `REMARK_CODE`                   string COMMENT '',
    `ADD_INFO_CODE`                 string COMMENT '',
    `BIRTH_COUNTY`                  string COMMENT '',
    `BIRTH_STATE`                   string COMMENT '',
    `MIDDLE_NAME_2`                 string COMMENT '',
    `TITLE`                         string COMMENT '',
    `AGE`                           smallint COMMENT '',
    `CREATE_USER_ID`                string COMMENT '',
    `LAST_NAME_ALPHA_KEY`           string COMMENT '',
    `CREATE_DATETIME`               timestamp COMMENT '',
    `NAME_SEQUENCE`                 string COMMENT '',
    `AUDIT_TIMESTAMP`               timestamp COMMENT '',
    `AUDIT_USER_ID`                 string COMMENT '',
    `AUDIT_MODULE_NAME`             string COMMENT '',
    `AUDIT_CLIENT_USER_ID`          string COMMENT '',
    `AUDIT_CLIENT_IP_ADDRESS`       string COMMENT '',
    `AUDIT_CLIENT_WORKSTATION_NAME` string COMMENT '',
    `AUDIT_ADDITIONAL_INFO`         string COMMENT ''
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://dpr-dms-raw-zone-development/OMS_OWNER/OFFENDERS/'
    TBLPROPERTIES (
        'classification' = 'parquet')