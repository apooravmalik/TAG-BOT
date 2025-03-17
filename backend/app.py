from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import requests
from sqlalchemy.orm import Session
from config.database import get_db
from sqlalchemy import text
import re

# FastAPI Instance
app = FastAPI()

# Ollama API Configuration
OLLAMA_URL = "http://localhost:11500/api/generate"
MODEL_NAME = "sql_gen"  # Updated model name to use your fine-tuned model

# Request Model
class QueryRequest(BaseModel):
    query: str  # Natural language query from user
    table_name: str  # Table name to fetch schema dynamically

# SQL Standardization Function
def standardize_sql(sql_query):
    # Dictionary of replacements for table names (case-insensitive)
    table_replacements = {
        r'\[response\]': 'response',
        r'\[workflow\]': 'workflow',
        r'\[incident\]': 'incident',
        r'\[user\]': 'user',
        r'response': 'response',
        r'workflow': 'workflow',
        r'incident': 'incident',
        r'user': 'user',
    }
    
    # Dictionary of replacements for column names
    column_replacements = {
        r'LastUpdateDate': 'updated_at',
        r'Created': 'created_at',
        r'UpdatedAt': 'updated_at',
        r'CreatedAt': 'created_at',
        r'last_login': 'last_login_at',
        r'last_updated': 'updated_at',
        r'date_created_at': 'created_at',
        r'textMME': 'text_mme',
    }
    
    # Dictionary of replacements for MSSQL specific syntax
    syntax_replacements = {
        r'CURRENT_DATE - INTERVAL \'(\d+)\' DAY': r'DATEADD(day, -\1, GETDATE())',
        r'DATE\([\w_]+\) > CURRENT_DATE - INTERVAL': r'created_at >= DATEADD(day, -',
        r'NOW\(\)': 'GETDATE()',
        r'CURRENT_DATE': 'CAST(GETDATE() AS DATE)',
    }
    
    # Apply table name replacements (case-insensitive)
    for pattern, replacement in table_replacements.items():
        sql_query = re.sub(r'FROM\s+' + pattern, f'FROM {replacement}', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'JOIN\s+' + pattern, f'JOIN {replacement}', sql_query, flags=re.IGNORECASE)
    
    # Apply column name replacements
    for pattern, replacement in column_replacements.items():
        sql_query = re.sub(pattern, replacement, sql_query, flags=re.IGNORECASE)
    
    # Apply syntax replacements
    for pattern, replacement in syntax_replacements.items():
        sql_query = re.sub(pattern, replacement, sql_query)
    
    # Ensure each query ends with a semicolon
    if not sql_query.strip().endswith(';'):
        sql_query = sql_query.strip() + ';'
    
    return sql_query

# Function to fetch table schema dynamically
def get_table_schema(db: Session, table_name: str) -> dict:
    try:
        # Explicitly define the SQL query using text()
        query = text("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :table_name")

        # Execute query with parameters safely
        result = db.execute(query, {"table_name": table_name}).all()

        if not result:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in database")

        # Convert to JSON format
        schema = {
            "table_name": table_name,
            "columns": [
                {"name": row.COLUMN_NAME, "type": row.DATA_TYPE, "nullable": row.IS_NULLABLE}
                for row in result
            ]
        }
        return schema

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching schema: {str(e)}")
                    
# Function to call Ollama API to generate SQL
def generate_sql(natural_query: str, schema: dict) -> str:
    # Create a schema-formatted prompt
    table_name = schema["table_name"]
    columns_info = "\n".join([f"- {col['name']} ({col['type']}, {'NULL' if col['nullable'] == 'YES' else 'NOT NULL'})" 
                            for col in schema["columns"]])
    
    schema_prompt = f"""
    Table: {table_name}
    Columns:
    {columns_info}
    """
    
    payload = {
        "model": MODEL_NAME,
        "prompt": f"""You are a SQL code generator. Output ONLY valid SQL code with no explanations.

Request: Convert this into MSSQL: {natural_query}

Table Schema:
{schema_prompt}

SQL:""",
        "stream": False,
        "temperature": 0.0,
        "top_p": 1.0,
        "repetition_penalty": 1.2
    }
    
    try:
        response = requests.post(OLLAMA_URL, json=payload)
        response.raise_for_status()
        raw_response = response.json().get("response", "").strip()
        
        # Extract SQL from code blocks if present
        if "```" in raw_response:
            sql_match = re.search(r"```(?:sql)?\s*(.*?)\s*```", raw_response, re.DOTALL)
            if sql_match:
                raw_response = sql_match.group(1).strip()
        
        # Apply additional standardization
        raw_response = standardize_sql(raw_response)
        
        print(f"Final SQL: {raw_response}")
        return raw_response
        
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Ollama API Error: {str(e)}")

# Function to convert SQL result to natural language
def convert_to_natural_language(sql_result: str) -> str:
    payload = {
        "model": MODEL_NAME,
        "prompt": f"Convert the following SQL output into a natural language response that a non-technical person would understand: {sql_result}",
        "stream": False
    }
    try:
        response = requests.post(OLLAMA_URL, json=payload)
        response.raise_for_status()
        return response.json().get("response", "").strip()
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Ollama API Error: {str(e)}")

# Endpoint to get table schema dynamically
@app.get("/schema/{table_name}")
async def fetch_schema(table_name: str, db: Session = Depends(get_db)):
    return get_table_schema(db, table_name)

# FastAPI Endpoint to handle user queries
@app.post("/query")
async def process_query(request: QueryRequest, db: Session = Depends(get_db)):
    try:
        # Step 1: Fetch table schema dynamically
        table_schema = get_table_schema(db, request.table_name)
        print(f"Schema fetched successfully: {table_schema}")

        # Step 2: Convert natural language to SQL
        sql_query = generate_sql(request.query, table_schema)
        print(f"Generated SQL: {sql_query}")
        
        if not sql_query:
            raise HTTPException(status_code=400, detail="Failed to generate SQL query")

        # Step 3: Execute SQL in database
        try:
            # Create a text object from the SQL string
            sql = text(sql_query)
            
            # Execute the query safely
            result = db.execute(sql)
            sql_result = result.all()
            
            if not sql_result:
                return {"query": request.query, "sql": sql_query, "response": "No results found for your query."}
                
            # Convert SQLAlchemy result to a serializable format
            formatted_result = [dict(row._mapping) for row in sql_result]
            print(f"Query executed successfully with {len(formatted_result)} results")
            
            # Step 4: Convert SQL result to natural language
            final_response = convert_to_natural_language(str(formatted_result))
            return {"query": request.query, "sql": sql_query, "response": final_response}
            
        except Exception as e:
            db.rollback()
            print(f"SQL Execution Error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"SQL Execution Error: {str(e)}")

    except HTTPException as e:
        # Re-raise HTTP exceptions directly
        raise e
    except Exception as e:
        # Print detailed error information
        import traceback
        print(f"Unexpected Error: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {str(e)}")