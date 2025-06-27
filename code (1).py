import pandas as pd
import boto3
import uuid
import time

# --- Configuration ---
S3_BUCKET_NAME = "my-athena-data-bucket-12345"
S3_PREFIX = "my_table_data/"
S3_OUTPUT_LOCATION = f"s3://{S3_BUCKET_NAME}/athena-query-results/"
ATHENA_DATABASE = "my_database"
ATHENA_TABLE = "sales_records"

# boto3 clients
glue_client = boto3.client('glue')
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')

# --- Helper function to check if table exists ---
def table_exists(database, table):
    try:
        glue_client.get_table(DatabaseName=database, Name=table)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False

# --- Helper function to run and wait for Athena query ---
def run_athena_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION}
    )
    query_execution_id = response['QueryExecutionId']
    
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if state == 'FAILED':
                print("Query FAILED:", status['QueryExecution']['Status']['StateChangeReason'])
            return
        time.sleep(2)

# --- Main Logic ---

# 1. Create DataFrame
df_new_records = pd.DataFrame({
    'order_id': [201], 'product_name': ['Monitor'], 'quantity': [1], 'sale_price': [350.0]
})

# 2. Check if table exists, create if not
if not table_exists(ATHENA_DATABASE, ATHENA_TABLE):
    print(f"Table `{ATHENA_DATABASE}`.`{ATHENA_TABLE}` does not exist. Creating it...")
    
    # NOTE: Manually defining the schema is brittle. awswrangler infers this.
    create_table_ddl = f"""
    CREATE EXTERNAL TABLE `{ATHENA_DATABASE}`.`{ATHENA_TABLE}`(
      `order_id` int, 
      `product_name` string, 
      `quantity` int, 
      `sale_price` double)
    STORED AS PARQUET
    LOCATION 's3://{S3_BUCKET_NAME}/{S3_PREFIX}'
    tblproperties ('parquet.compress'='SNAPPY');
    """
    run_athena_query(create_table_ddl)
    print("Table created.")
else:
    print(f"Table `{ATHENA_DATABASE}`.`{ATHENA_TABLE}` already exists. Appending data.")

# 3. Convert DataFrame to Parquet and upload to S3
# Generate a unique filename to avoid collisions
file_name = f"{uuid.uuid4()}.parquet"
s3_key = f"{S3_PREFIX}{file_name}"

# Convert to parquet in-memory
parquet_buffer = df_new_records.to_parquet(index=False, engine='pyarrow')

s3_client.put_object(
    Bucket=S3_BUCKET_NAME,
    Key=s3_key,
    Body=parquet_buffer
)
print(f"Uploaded data to s3://{S3_BUCKET_NAME}/{s3_key}")

# 4. If your table is partitioned, you need to refresh them.
# For non-partitioned tables, this isn't strictly necessary but good practice.
# For partitioned tables, it is MANDATORY.
# print("Running MSCK REPAIR TABLE to discover new data/partitions...")
# run_athena_query(f"MSCK REPAIR TABLE `{ATHENA_TABLE}`")
# print("Table repaired.")
