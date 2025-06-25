import pandas as pd
import awswrangler as wr
from datetime import date

# --- 1. Configuration ---
# Replace with your specific AWS details
S3_STAGING_DIR = "s3://your-athena-query-results-bucket/path/"
ATHENA_DATABASE = "your_athena_database_name"
TARGET_TABLE = "your_final_target_table_name"

# --- 2. Define Your SQL Queries ---
# Each query should return exactly one row
sql_query_1 = "SELECT 'value_a1' AS col_a, 100 AS col_b, 'value_c1' AS col_c"
sql_query_2 = "SELECT 'value_a2' AS col_a, 200 AS col_b, 'value_c2' AS col_c"
sql_query_3 = "SELECT 'value_a3' AS col_a, 300 AS col_b, 'value_c3' AS col_c"

queries = [sql_query_1, sql_query_2, sql_query_3]

print("Starting process...")

try:
    # --- 3. Run Queries and Collect DataFrames ---
    # We will run each query and store its resulting DataFrame in a list
    list_of_dfs = []
    for i, query in enumerate(queries, 1):
        print(f"Running query {i}...")
        df = wr.athena.read_sql_query(
            sql=query,
            database=ATHENA_DATABASE,
            s3_output=S3_STAGING_DIR
        )
        list_of_dfs.append(df)
        print(f"Query {i} finished, received 1 row.")

    # --- 4. Concatenate the Results ---
    # Combine the three 1-row DataFrames into a single 3-row DataFrame
    # ignore_index=True resets the index to be a clean 0, 1, 2
    final_df = pd.concat(list_of_dfs, ignore_index=True)

    print("\nOriginal concatenated data:")
    print(final_df)

    # --- 5. Add New Columns ---
    # Add today's date column.
    # It's good practice to name it like a partition key, e.g., 'run_date' or 'dt'
    final_df['run_date'] = date.today().strftime('%Y-%m-%d')

    # Add the flag column with your specified values
    # This works because the list has 3 items and the DataFrame has 3 rows.
    final_df['flag'] = ['I', 'B', 'C']

    print("\nData after adding new columns:")
    print(final_df)

    # --- 6. Append Data to the Target Athena Table ---
    # This function writes the DataFrame to S3 (in Parquet format by default)
    # and ensures the Athena table metadata is updated.
    
    # IMPORTANT: Ensure the column names and data types in `final_df` match your
    # existing Athena table `TARGET_TABLE`.
    
    # If your table is partitioned (e.g., by the date), specify it here.
    # This is highly recommended for performance.
    partition_cols = ['run_date']

    print(f"\nAppending {len(final_df)} rows to Athena table: {ATHENA_DATABASE}.{TARGET_TABLE}")

    response = wr.s3.to_parquet(
        df=final_df,
        path=f"s3://your-table-data-bucket/path/to/{TARGET_TABLE}/", # The S3 path where the table's data is stored
        dataset=True,
        database=ATHENA_DATABASE,
        table=TARGET_TABLE,
        mode="append",  # This is the key to adding new data without deleting old data
        partition_cols=partition_cols # Remove if your table is not partitioned
    )
    
    print("\nProcess finished successfully!")
    print("Data appended. Response from awswrangler:")
    print(response)

except Exception as e:
    print(f"An error occurred: {e}")
