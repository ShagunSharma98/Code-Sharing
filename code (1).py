from langchain.agents import AgentExecutor, create_react_agent, Tool
from langchain_openai import ChatOpenAI
from langchain import hub

# 1. Define the tools for the agent
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

# 2. Get the ReAct agent prompt template
# This prompt is engineered to make the LLM think in a "Thought, Action, Observation" loop
prompt = hub.pull("hwchase17/react")

# 3. Initialize the LLM
llm = ChatOpenAI(temperature=0, model_name='gpt-4')

# 4. Create the agent
agent = create_react_agent(llm, tools, prompt)

# 5. Create the Agent Executor (the runtime for the agent)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, handle_parsing_errors=True)

# --- This is how you address Requirement #3: Getting References ---
# By setting `verbose=True`, the agent's thought process is printed.
# For a production app, you would capture this using "Callbacks".
# It will show the exact tools called, the inputs (like the SQL query), and the outputs.
