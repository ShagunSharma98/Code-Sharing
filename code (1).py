import boto3
import pandas as pd
import time
from langchain.agents import AgentExecutor, create_react_agent, Tool
from langchain import hub

# ===================================================================
# NEW: Import the Bedrock LLM from langchain-aws
# ===================================================================
from langchain_aws import ChatBedrock

# --- Tool Definitions (UNCHANGED) ---
# The functions for get_table_schema and run_athena_query are exactly the same as before.

def get_table_schema(table_name: str, database: str = 'your_glue_database') -> str:
    """Retrieves the DDL schema for a specific table in the AWS Glue Data Catalog."""
    # ... (same code as before)
    try:
        glue_client = boto3.client('glue')
        response = glue_client.get_table(DatabaseName=database, Name=table_name)
        columns = response['Table']['StorageDescriptor']['Columns']
        schema_info = f"Schema for table '{table_name}':\n"
        for col in columns:
            schema_info += f"- {col['Name']}: {col['Type']}\n"
        return schema_info
    except Exception as e:
        return f"Error: Could not get schema for table {table_name}. {str(e)}"

def run_athena_query(query: str, database: str = 'your_glue_database', s3_output_location: str = 's3://your-athena-query-results-bucket/') -> pd.DataFrame:
    """Runs a SQL query using AWS Athena and returns the result as a pandas DataFrame."""
    # ... (same code as before)
    athena_client = boto3.client('athena')
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': s3_output_location}
        )
        query_execution_id = response['QueryExecutionId']
        state = 'RUNNING'
        while state in ['RUNNING', 'QUEUED']:
            time.sleep(2)
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = result['QueryExecution']['Status']['State']
            if state == 'FAILED': return f"Query failed: {result['QueryExecution']['Status']['StateChangeReason']}"
            elif state == 'CANCELLED': return "Query was cancelled."
        result_paginator = athena_client.get_paginator('get_query_results')
        result_iter = result_paginator.paginate(QueryExecutionId=query_execution_id)
        rows, column_info = [], None
        for result_page in result_iter:
            if not column_info: column_info = [col['Name'] for col in result_page['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            for row in result_page['ResultSet']['Rows'][1:]: rows.append([item.get('VarCharValue') for item in row['Data']])
        return pd.DataFrame(rows, columns=column_info)
    except Exception as e:
        return f"Error running query: {str(e)}"


# --- Agent Assembly (The only part that changes is the LLM initialization) ---

# 1. Define the tools for the agent (UNCHANGED)
tools = [
    Tool(
        name="SchemaInspector",
        func=get_table_schema,
        description="Useful for getting the schema and column details of a specific database table. Input should be the table name.",
    ),
    Tool(
        name="AthenaQueryExecutor",
        func=run_athena_query,
        description="Useful for executing an Athena SQL query to get data from the database. Input should be a complete and valid SQL query.",
    ),
]

# 2. Get the ReAct agent prompt template (UNCHANGED)
prompt = hub.pull("hwchase17/react")

# ===================================================================
# 3. Initialize the LLM (THIS IS THE TRANSFORMED PART)
# ===================================================================

# BEFORE (OpenAI):
# from langchain_openai import ChatOpenAI
# llm = ChatOpenAI(temperature=0, model_name='gpt-4')

# AFTER (Amazon Bedrock):
# Ensure your environment is configured with AWS credentials
bedrock_client = boto3.client(service_name="bedrock-runtime", region_name="us-east-1") # Or your preferred region

# Choose your model
model_id = "anthropic.claude-3-sonnet-20240229-v1:0"
# Other great options:
# "anthropic.claude-3-haiku-20240307-v1:0" (faster, cheaper)
# "meta.llama3-8b-instruct-v1:0" (excellent open model)

llm = ChatBedrock(
    client=bedrock_client,
    model_id=model_id,
    model_kwargs={
        "temperature": 0.0, # Use 0 for deterministic, factual responses
        "max_tokens": 4096,
    }
)

# ===================================================================
# The rest of the agent setup is identical
# ===================================================================

# 4. Create the agent (UNCHANGED)
agent = create_react_agent(llm, tools, prompt)

# 5. Create the Agent Executor (UNCHANGED)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, handle_parsing_errors=True)

# --- Run a Query (UNCHANGED) ---
user_query = "How many orders did we have in the 'electronics' category last month? And what was the total sales amount?"
response = agent_executor.invoke({"input": user_query})

print("\n--- Final Answer ---")
print(response['output'])
