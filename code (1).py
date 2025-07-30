import boto3
import pandas as pd
import time

# --- Tool 1: Schema Inspector ---
def get_table_schema(table_name: str, database: str = 'your_glue_database') -> str:
    """
    Retrieves the DDL schema for a specific table in the AWS Glue Data Catalog.
    Use this to understand the columns and data types of a table.
    """
    try:
        glue_client = boto3.client('glue')
        response = glue_client.get_table(DatabaseName=database, Name=table_name)
        # Format the schema nicely for the LLM
        columns = response['Table']['StorageDescriptor']['Columns']
        schema_info = f"Schema for table '{table_name}':\n"
        for col in columns:
            schema_info += f"- {col['Name']}: {col['Type']}\n"
        return schema_info
    except Exception as e:
        return f"Error: Could not get schema for table {table_name}. {str(e)}"

# --- Tool 2: Athena Query Executor ---
def run_athena_query(query: str, database: str = 'your_glue_database', s3_output_location: str = 's3://your-athena-query-results-bucket/') -> pd.DataFrame:
    """
    Runs a SQL query using AWS Athena and returns the result as a pandas DataFrame.
    Use this to query the data.
    """
    athena_client = boto3.client('athena')
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': s3_output_location}
        )
        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        state = 'RUNNING'
        while state in ['RUNNING', 'QUEUED']:
            time.sleep(2)
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = result['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return f"Query failed: {result['QueryExecution']['Status']['StateChangeReason']}"
            elif state == 'CANCELLED':
                return "Query was cancelled."

        # Fetch results
        result_paginator = athena_client.get_paginator('get_query_results')
        result_iter = result_paginator.paginate(QueryExecutionId=query_execution_id)
        
        rows = []
        column_info = None
        for result_page in result_iter:
            if not column_info:
                column_info = [col['Name'] for col in result_page['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            for row in result_page['ResultSet']['Rows'][1:]: # Skip header row
                rows.append([item.get('VarCharValue') for item in row['Data']])
        
        return pd.DataFrame(rows, columns=column_info)

    except Exception as e:
        return f"Error running query: {str(e)}"
