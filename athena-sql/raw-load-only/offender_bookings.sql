CREATE EXTERNAL TABLE `raw`.`nomis_offender_bookings_load_only`
(
    `Op`                            string COMMENT '',
    `_timestamp`                    string COMMENT '',
    `OFFENDER_BOOK_ID`              decimal(10,0) COMMENT '',
    `BOOKING_BEGIN_DATE`            timestamp COMMENT '',
    `BOOKING_END_DATE`              timestamp COMMENT '',
    `BOOKING_NO`                    string COMMENT '',
    `OFFENDER_ID`                   decimal(10,0) COMMENT '',
    `AGY_LOC_ID`                    string COMMENT '',
    `LIVING_UNIT_ID`                decimal(10,0) COMMENT '',
    `DISCLOSURE_FLAG`               string COMMENT '',
    `IN_OUT_STATUS`                 string COMMENT '',
    `ACTIVE_FLAG`                   string COMMENT '',
    `BOOKING_STATUS`                string COMMENT '',
    `YOUTH_ADULT_CODE`              string COMMENT '',
    `FINGER_PRINTED_STAFF_ID`       decimal(10,0) COMMENT '',
    `SEARCH_STAFF_ID`               decimal(10,0) COMMENT '',
    `PHOTO_TAKING_STAFF_ID`         decimal(10,0) COMMENT '',
    `ASSIGNED_STAFF_ID`             decimal(10,0) COMMENT '',
    `CREATE_AGY_LOC_ID`             string COMMENT '',
    `BOOKING_TYPE`                  string COMMENT '',
    `BOOKING_CREATED_DATE`          timestamp COMMENT '',
    `ROOT_OFFENDER_ID`              decimal(10,0) COMMENT '',
    `AGENCY_IML_ID`                 decimal(10,0) COMMENT '',
    `SERVICE_FEE_FLAG`              string COMMENT '',
    `EARNED_CREDIT_LEVEL`           string COMMENT '',
    `EKSTRAND_CREDIT_LEVEL`         string COMMENT '',
    `INTAKE_AGY_LOC_ID`             string COMMENT '',
    `ACTIVITY_DATE`                 timestamp COMMENT '',
    `INTAKE_CASELOAD_ID`            string COMMENT '',
    `INTAKE_USER_ID`                string COMMENT '',
    `CASE_OFFICER_ID`               integer COMMENT '',
    `CASE_DATE`                     timestamp COMMENT '',
    `CASE_TIME`                     timestamp COMMENT '',
    `COMMUNITY_ACTIVE_FLAG`         string COMMENT '',
    `CREATE_INTAKE_AGY_LOC_ID`      string COMMENT '',
    `COMM_STAFF_ID`                 decimal(10,0) COMMENT '',
    `COMM_STATUS`                   string COMMENT '',
    `COMMUNITY_AGY_LOC_ID`          string COMMENT '',
    `NO_COMM_AGY_LOC_ID`            integer COMMENT '',
    `COMM_STAFF_ROLE`               string COMMENT '',
    `AGY_LOC_ID_LIST`               string COMMENT '',
    `STATUS_REASON`                 string COMMENT '',
    `TOTAL_UNEXCUSED_ABSENCES`      integer COMMENT '',
    `REQUEST_NAME`                  string COMMENT '',
    `CREATE_DATETIME`               timestamp COMMENT '',
    `CREATE_USER_ID`                string COMMENT '',
    `MODIFY_DATETIME`               timestamp COMMENT '',
    `MODIFY_USER_ID`                string COMMENT '',
    `RECORD_USER_ID`                string COMMENT '',
    `INTAKE_AGY_LOC_ASSIGN_DATE`    timestamp COMMENT '',
    `AUDIT_TIMESTAMP`               timestamp COMMENT '',
    `AUDIT_USER_ID`                 string COMMENT '',
    `AUDIT_MODULE_NAME`             string COMMENT '',
    `AUDIT_CLIENT_USER_ID`          string COMMENT '',
    `AUDIT_CLIENT_IP_ADDRESS`       string COMMENT '',
    `AUDIT_CLIENT_WORKSTATION_NAME` string COMMENT '',
    `AUDIT_ADDITIONAL_INFO`         string COMMENT '',
    `BOOKING_SEQ`                   integer COMMENT '',
    `ADMISSION_REASON`              string COMMENT ''
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
        's3://dpr-raw-zone-test/nomis/offender_bookings/_symlink_format_load_only_manifest'
    TBLPROPERTIES (
        'classification' = 'parquet')