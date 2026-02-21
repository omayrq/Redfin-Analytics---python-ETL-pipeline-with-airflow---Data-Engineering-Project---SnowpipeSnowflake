// REDFIN ANALYTICS SNOWFLAKE DATA WAREHOUSE CREATION SCRIPT

--------------------- Create database, schema, and destination table -------------------

--- Create the database, warehouse, and schema
DROP DATABASE IF EXISTS redfin_analytics_database;
CREATE OR REPLACE DATABASE redfin_analytics_database;
CREATE OR REPLACE WAREHOUSE redfin_analytics_warehouse;
CREATE OR REPLACE SCHEMA redfin_analytics_schema;

--- Create the destination table in the schema
DROP TABLE IF EXISTS redfin_analytics_database.redfin_analytics_schema.redfin_analytics_table;
CREATE OR REPLACE TABLE redfin_analytics_database.redfin_analytics_schema.redfin_analytics_table (
period_begin DATE,
period_end DATE,
period_duration INT,
region_type STRING,
region_type_id INT,
table_id INT,
is_seasonally_adjusted STRING,
city STRING,
state STRING,
state_code STRING,
property_type STRING,
property_type_id INT,
median_sale_price FLOAT,
median_list_price FLOAT,
median_ppsf FLOAT,
median_list_ppsf FLOAT,
homes_sold FLOAT,
inventory FLOAT,
months_of_supply FLOAT,
median_dom FLOAT,
avg_sale_to_list FLOAT,
sold_above_list FLOAT,
parent_metro_region_metro_code STRING,
last_updated DATETIME,
period_begin_in_years STRING,
period_end_in_years STRING,
period_begin_in_months STRING,
period_end_in_months STRING
);

----------------------- Create CSV file format and staging area --------------------

// Create file format object
CREATE OR REPLACE SCHEMA file_format_schema;
CREATE OR REPLACE FILE FORMAT redfin_analytics_database.file_format_schema.csv_format
    type = 'CSV'
    field_delimiter = ','
    RECORD_DELIMITER = '\n'
    skip_header = 1
    error_on_column_count_mismatch = TRUE;
    
// Create the staging area
CREATE OR REPLACE SCHEMA external_stage_schema;
CREATE OR REPLACE STAGE redfin_analytics_database.external_stage_schema.redfin_dw_ext_stage 
    url="s3://redfin-analytics-transformed-data-bucket/"
    credentials=(aws_key_id='AKIAXXXXXXXXXXXXXXXX'           --- AWS IAM User Access key
    aws_secret_key='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')    --- AWS IAM User Secret Access key
    FILE_FORMAT = redfin_analytics_database.file_format_schema.csv_format;

-------------------------------- Create the Snowpipe ---------------------------

// Create the Snowpipe
CREATE OR REPLACE SCHEMA redfin_analytics_database.snowpipe_schema;
CREATE OR REPLACE PIPE redfin_analytics_database.snowpipe_schema.redfin_analytics_snowpipe
auto_ingest = TRUE
AS 
COPY INTO redfin_analytics_database.redfin_analytics_schema.redfin_analytics_table
FROM @redfin_analytics_database.external_stage_schema.redfin_dw_ext_stage;

------------------------------------ END OF SCRIPT --------------------------------