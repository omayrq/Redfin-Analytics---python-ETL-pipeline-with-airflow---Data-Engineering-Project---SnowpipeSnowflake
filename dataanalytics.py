import pandas as pd
import boto3  # Required for s3_client
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator # FIX: Missing import from image_f76c37.jpg

# --- Initialize AWS Client ---
# Note: Ensure your keys from image_f4c085.jpg are configured in your environment
s3_client = boto3.client('s3')
target_bucket_name = 's3-redfin-transformed-mua' 
url_by_city = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'

# --- Task 1: Extraction ---
def extract_data(**kwargs):
    url = kwargs['url']
    df = pd.read_csv(url, compression='gzip', sep='\t', nrows=50)  # Adjust nrows as needed for testing
    now = datetime.now()
    date_now_string = now.strftime("%d%m%Y%H%M%S")
    file_str = 'redfin_data_' + date_now_string
    df.to_csv(f"{file_str}.csv", index=False)
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    return [output_file_path, file_str]

# --- Task 2: Transformation ---

def transform_data(**kwargs):
    ti = kwargs['ti']

    data = ti.xcom_pull(task_ids="tsk_extract_redfin_data")[0]
    object_key = ti.xcom_pull(task_ids="tsk_extract_redfin_data")[1]

    df = pd.read_csv(data)
    df.columns = [c.lower() for c in df.columns]

    print("Columns:", df.columns.tolist())

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

    # limit again for safety
    df = df.head(50)

    if 'period_begin' in df.columns:
        df['period_begin'] = pd.to_datetime(df['period_begin'])
        df["period_begin_year"] = df['period_begin'].dt.year
        df["period_begin_month"] = df['period_begin'].dt.month

    if 'period_end' in df.columns:
        df['period_end'] = pd.to_datetime(df['period_end'])
        df["period_end_year"] = df['period_end'].dt.year
        df["period_end_month"] = df['period_end'].dt.month

    csv_data = df.to_csv(index=False)

    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=target_bucket_name,
        Key=f"{object_key}.csv",
        Body=csv_data
    )

# --- DAG Definition ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 2, 20),
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG('redfin_analytics_dag', default_args=default_args, catchup=False) as dag:

    extract_redfin_data = PythonOperator(
        task_id='tsk_extract_redfin_data',
        python_callable=extract_data,
        op_kwargs={'url': url_by_city}
    )

    transform_redfin_data = PythonOperator(
        task_id='tsk_transform_redfin_data',
        python_callable=transform_data
    )

    load_to_s3 = BashOperator(
        task_id='tsk_load_to_s3',
        bash_command='aws s3 mv {{ ti.xcom_pull("tsk_extract_redfin_data")[0] }} s3://s3-store-raw-data-mua'
    )

    # Set Dependencies
    extract_redfin_data >> transform_redfin_data >> load_to_s3