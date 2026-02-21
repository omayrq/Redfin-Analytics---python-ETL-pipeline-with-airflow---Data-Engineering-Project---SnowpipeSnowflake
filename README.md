Redfin Analytics: Python ETL Pipeline with Airflow - Data Engineering Project

This project demonstrates building an end-to-end ETL (Extract, Transform, Load) pipeline to fetch real estate data from Redfin. The pipeline uses Apache Airflow for orchestration, AWS EC2 for hosting, AWS S3 for storage, Snowflake for data warehousing with Snowpipe for automated ingestion, and Power BI for visualization.

The data is extracted from Redfin's public market tracker dataset, transformed using Pandas, stored in S3 buckets, ingested into Snowflake via Snowpipe triggered by S3 events, and finally visualized in Power BI.
Prerequisites

AWS account (free tier eligible for most steps).
Basic knowledge of AWS, Python, Airflow, Snowflake, and Power BI.
VS Code with Remote-SSH extension installed.
Git Bash or a terminal for SSH commands.

Step 1: Set Up AWS EC2 Instance

Log in to your AWS Console.
Navigate to EC2 > Instances > Launch Instance.
Configure the instance:
Name: EC2-redfin-endyoutube-mua.
AMI (Amazon Machine Image): Ubuntu Server (Quick Start > Ubuntu).
Instance Type: t3.xlarge.
Key Pair: Create a new key pair or use an existing one. Download the .pem file (e.g., mua-dev.pem) and store it in your project directory.
Network Settings:
Allow HTTPS traffic from the internet.
Allow HTTP traffic from the internet.


Launch the instance.
Once the instance is running, select it and click Connect.
Use the SSH Client tab to get the connection command.
In your local terminal (e.g., Git Bash):
Run chmod 400 "mua-dev.pem" (replace with your key file name).
Run the SSH command, e.g., ssh -i "mua-dev.pem" ubuntu@ec2-98-86-231-15.compute-1.amazonaws.com.


Step 2: Install Dependencies on EC2
In the SSH terminal on the EC2 instance, run the following commands:
textsudo apt update
sudo apt install python3-pip
sudo apt install python3-venv
python3 -m venv redfin_venv
source redfin_venv/bin/activate
pip install pandas
pip install boto3
pip install --upgrade awscli
pip install apache-airflow
airflow version
Step 3: Create AWS Access Keys

In AWS Console, navigate to IAM > Users > Select your user > Security credentials > Create access key.
Note down the Access Key ID and Secret Access Key.
Back in the EC2 terminal:textaws configure
Enter Access Key ID.
Enter Secret Access Key.
Enter default region (e.g., us-east-1).
Leave output format as default.


Step 4: Set Up Airflow

In the EC2 terminal:textairflow standalone
Airflow will generate a password. To view it:
Open a second terminal window connected to EC2.
Run cat /home/ubuntu/airflow/simple_auth_manager_passwords.json.generated.
Username: admin, Password: From the file.

In AWS Console, get the instance's Public IPv4 address (e.g., 52.90.47.205).
Open in browser: http://52.90.47.205:8080/ (use HTTP if HTTPS fails).
If the page doesn't load, edit security groups:
Select instance > Security > Security groups hyperlink.
Edit inbound rules > Add rule: Type = Custom TCP, Port = 8080, Source = Anywhere IPv4.
Save rules.

Log in to Airflow UI with admin and the generated password.
Click DAGs to view built-in DAGs.

Step 5: Set Up VS Code Remote Connection

In local VS Code, install the Remote-SSH extension if not already.
Open VS Code terminal or command palette (Ctrl+Shift+P) > Type "Remote-SSH: Connect to Host".
Configure SSH config file (~/.ssh/config or equivalent):text# Read more about SSH config files: https://linux.die.net/man/5/ssh_config
Host EC2-redfin-endyoutube-mua
    HostName 52.90.47.205
    User ubuntu
    IdentityFile C:\Users\MYC\Downloads\mua-dev.pem
Connect to the host.
Select "Linux" as the platform.
Open the /home/ubuntu folder to access files.

Step 6: Create Airflow DAG

In VS Code (remote), navigate to /home/ubuntu/airflow.
Create a dags folder if it doesn't exist.
Inside dags, create redfin_analytics.py with the following code:

Pythonimport pandas as pd
import boto3
from datetime import datetime, timedelta
from airflow import DAG
# Updated Imports to remove deprecation warnings
from airflow.providers.amazon.operators.python import PythonOperator  # Note: Corrected to amazon provider if needed; adjust based on version
from airflow.providers.amazon.operators.bash import BashOperator      # Note: Corrected to amazon provider if needed; adjust based on version

# Constants
TARGET_BUCKET_TRANSFORMED = 's3-redfin-transformed-mua' 
TARGET_BUCKET_RAW = 's3-store-raw-data-mua'
URL_BY_CITY = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'

def extract_data(**kwargs):
    df = pd.read_csv(URL_BY_CITY, compression='gzip', sep='\t', nrows=1000)
    file_str = f'redfin_data_{datetime.now().strftime("%d%m%Y%H%M%S")}'
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    
    df.to_csv(output_file_path, index=False)
    return [output_file_path, file_str]

def transform_data(**kwargs):
    ti = kwargs['ti']
    # Pulling metadata from extract task
    extract_info = ti.xcom_pull(task_ids="tsk_extract_redfin_data")
    local_path = extract_info[0]
    object_key = extract_info[1]

    df = pd.read_csv(local_path)
    df.columns = [c.lower() for c in df.columns]

    # Transformation Logic
    if 'city' in df.columns:
        df['city'] = df['city'].astype(str).str.replace(',', '')

    cols = [
        'period_begin','period_end','period_duration','region_type','region_type_id','table_id',
        'is_seasonally_adjusted','city','state','state_code','property_type','property_type_id',
        'median_sale_price','median_list_price','median_ppsf','median_list_ppsf','homes_sold',
        'inventory','months_of_supply','median_dom','avg_sale_to_list','sold_above_list',
        'parent_metro_region_metro_code','last_updated'
    ]
    
    existing_cols = [c for c in cols if c in df.columns]
    df = df[existing_cols].dropna()

    for col in ['period_begin', 'period_end']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
            df[f"{col}_year"] = df[col].dt.year
            df[f"{col}_month"] = df[col].dt.month

    # Upload Transformed Data
    csv_data = df.to_csv(index=False)
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=TARGET_BUCKET_TRANSFORMED,
        Key=f"{object_key}.csv",
        Body=csv_data
    )

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 2, 20),
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG('redfin_analytics_dag', default_args=default_args, catchup=False) as dag:

    extract_redfin_data = PythonOperator(
        task_id='tsk_extract_redfin_data',
        python_callable=extract_data
    )

    transform_redfin_data = PythonOperator(
        task_id='tsk_transform_redfin_data',
        python_callable=transform_data
    )

    load_to_s3_raw = BashOperator(
        task_id='tsk_load_to_s3_raw',
        bash_command=f'aws s3 mv {{{{ ti.xcom_pull("tsk_extract_redfin_data")[0] }}}} s3://{TARGET_BUCKET_RAW}/'
    )

    extract_redfin_data >> transform_redfin_data >> load_to_s3_raw
Note: The imports for operators may need adjustment based on your Airflow version (e.g., use airflow.operators.python if providers are not installed).
Step 7: Create S3 Buckets

In AWS Console, go to S3 > Create Bucket.
Create s3-redfin-transformed-mua.
Create s3-store-raw-data-mua.

Step 8: Trigger and Monitor DAG

In Airflow UI, search for redfin_analytics_dag.
Click on it > Switch to Graph view.
Click Trigger to run the DAG.
The DAG extracts data from Redfin, transforms it, uploads transformed CSV to s3-redfin-transformed-mua, and raw CSV to s3-store-raw-data-mua.
Verify success in Graph view and check S3 buckets for files.

Step 9: Set Up Snowflake

Sign up or log in to Snowflake: https://signup.snowflake.com/?trial=student&cloud=aws&region=us-west-2&utm_source=handsonessentials&utm_campaign=uni-dww#.
In Snowflake UI, go to Projects > Workspaces > Create new SQL worksheet named realestate_script.
Run the following SQL script:

SQL-- REDFIN ANALYTICS SNOWFLAKE DATA WAREHOUSE CREATION SCRIPT

--------------------- Create database, schema, and destination table -------------------

--- Create the database, warehouse, and schema
DROP DATABASE IF EXISTS redfin_analytics_database;
CREATE OR REPLACE DATABASE redfin_analytics_database;
--CREATE OR REPLACE WAREHOUSE redfin_analytics_warehouse;  -- Uncomment if needed
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
    period_begin_in_years STRING,  -- Note: Adjusted to STRING; change if needed
    period_end_in_years STRING,
    period_begin_in_months STRING,
    period_end_in_months STRING
);

----------------------- Create CSV file format and staging area --------------------

-- Create file format object
CREATE OR REPLACE SCHEMA file_format_schema;
CREATE OR REPLACE FILE FORMAT redfin_analytics_database.file_format_schema.csv_format
    type = 'CSV'
    field_delimiter = ','
    RECORD_DELIMITER = '\n'
    skip_header = 1
    error_on_column_count_mismatch = TRUE;
    
-- Create the staging area
CREATE OR REPLACE SCHEMA external_stage_schema;
CREATE OR REPLACE STAGE redfin_analytics_database.external_stage_schema.redfin_dw_ext_stage 
    url="s3://s3-redfin-transformed-mua/"
    credentials=(aws_key_id=''           --- AWS IAM User Access key
    aws_secret_key='')    --- AWS IAM User Secret Access key
    FILE_FORMAT = redfin_analytics_database.file_format_schema.csv_format;

LIST @redfin_analytics_database.external_stage_schema.redfin_dw_ext_stage;

-------------------------------- Create the Snowpipe ---------------------------

-- Create the Snowpipe
CREATE OR REPLACE SCHEMA redfin_analytics_database.snowpipe_schema;
CREATE OR REPLACE PIPE redfin_analytics_database.snowpipe_schema.redfin_analytics_snowpipe
    auto_ingest = TRUE
AS 
COPY INTO redfin_analytics_database.redfin_analytics_schema.redfin_analytics_table
FROM @redfin_analytics_database.external_stage_schema.redfin_dw_ext_stage;

DESC PIPE redfin_analytics_database.snowpipe_schema.redfin_analytics_snowpipe;
-- In the console, copy the "Notification Channel" (SQS ARN) for later use.

------------------------------------ END OF SCRIPT --------------------------------
Step 10: Test Snowflake
Run these queries in the worksheet:
SQLSELECT * FROM redfin_analytics_database.redfin_analytics_schema.redfin_analytics_table;

SELECT COUNT(*) FROM redfin_analytics_database.redfin_analytics_schema.redfin_analytics_table;
Step 11: Set Up S3 Event Notification for Snowpipe

In AWS S3, open s3-redfin-transformed-mua > Properties > Scroll to Event notifications.
Create new notification:
Name: redfin-snowpipe-event.
Event types: All object create events.
Destination: SQS Queue.
Enter the SQS ARN copied from Snowflake's DESC PIPE.

Save changes. Snowpipe will now trigger on new S3 objects.

Step 12: Visualize in Power BI

Open Power BI Desktop.
Get Data > Search for "Snowflake" > Connect.
Enter Snowflake server URL (copy from Snowflake account).
Warehouse: compute_wh.
Connect and load data from redfin_analytics_database.redfin_analytics_schema.redfin_analytics_table.
Create visualizations using charts (e.g., median sale price by city, trends over time).

Troubleshooting

Ensure AWS credentials have permissions for S3, EC2, and IAM.
If Airflow UI doesn't load, check security groups and instance status.
For Snowpipe issues, verify SQS ARN and IAM policies.
Data is limited to 1000 rows in extract for demo; remove nrows=1000 for full data.

License
This project is for educational purposes. Data from Redfin is public but check their terms.
Feel free to fork and contribute!
