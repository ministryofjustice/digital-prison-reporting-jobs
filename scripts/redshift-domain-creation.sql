-- =================================================================
-- Create a Spectrum link to the curated layer via the prisons 
-- hive catalogue
--
-- As each new table is deployed, this will
-- be automatically refreshed.
--
-- YOU NEED TO REPALCE <account> WITH THE ACCOUNT OF THE ENVIRONMENT
--
-- =================================================================
CREATE EXTERNAL SCHEMA prisons from data catalog
database 'prisons'
iam_role 'arn:aws:iam::<account>:role/dpr-redshift-spectrum-role'
create external database if not exists;


-- =================================================================
-- Create a schema space for domain
-- All domain views will be deployed here on a domain_table basis
--
-- =================================================================
CREATE SCHEMA IF NOT EXISTS domain QUOTA UNLIMITED;
GRANT USAGE ON SCHEMA domain TO dpruser;
