from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import requests
from sqlalchemy.orm import Session
from config.database import get_db  # Import session dependency
from sqlalchemy import text, TextClause, select

# FastAPI Instance
app = FastAPI()

# Ollama API Configuration
OLLAMA_URL = "http://localhost:11500/api/generate"
MODEL_NAME = "smollm2"  # Ensure this model is available in Ollama

# Request Model
class QueryRequest(BaseModel):
    query: str  # Natural language query from user
    table_name: str  # Table name to fetch schema dynamically

# Function to fetch table schema dynamically
def get_table_schema(db: Session, table_name: str) -> dict:
    try:
        # Explicitly define the SQL query using text()
        query = text("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :table_name")

        # Execute query with parameters safely
        result = db.execute(query, {"table_name": table_name}).all()  # Use .all() to fetch all results

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
    # Import re at the top level of the function
    import re
    
    payload = {
        "model": MODEL_NAME,
        "prompt": f"""Convert the following query into a valid Microsoft SQL Server query: {natural_query}.
        
        For queries asking to 'show all' or 'list all', simply use 'SELECT * FROM [table_name]' without any WHERE conditions.
        
        Use the following table schema: {schema}.
        
        CRITICAL RULES for SQL Server syntax:
        1. DO NOT use backticks (`) - use square brackets ([]) for table and column names if needed
        2. DO NOT use NOW() - use GETDATE() instead
        3. DO NOT use INTERVAL - use DATEADD() function instead (e.g., DATEADD(day, -1, GETDATE()))
        4. DO NOT use LIMIT - use TOP instead (e.g., SELECT TOP 10)
        5. DO NOT use EXTRACT(YEAR FROM date) - use YEAR(date) instead
        6. DO NOT use EXTRACT(MONTH FROM date) - use MONTH(date) instead
        7. DO NOT use EXTRACT(DAY FROM date) - use DAY(date) instead
        8. Format dates as 'YYYY-MM-DD' with single quotes
        
        Return ONLY the SQL query with no markdown, no comments, and no explanations.""",
        "stream": False
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
        
        # Replace common SQL syntax issues with SQL Server equivalents
        replacements = [
            (r"NOW\(\)", "GETDATE()"),
            (r"LIMIT\s+(\d+)", r"TOP \1"),
            (r"INTERVAL\s+(\d+)\s+DAY", r"day, -\1"),
            (r"`([^`]+)`", r"[\1]"),
            (r"\[\s*(\d{4}-\d{2}-\d{2}(?:\s\d{2}:\d{2}:\d{2})?)\s*\]", r"'\1'"),
            # Handle EXTRACT function
            (r"EXTRACT\s*\(\s*YEAR\s+FROM\s+([^)]+)\s*\)", r"YEAR(\1)"),
            (r"EXTRACT\s*\(\s*MONTH\s+FROM\s+([^)]+)\s*\)", r"MONTH(\1)"),
            (r"EXTRACT\s*\(\s*DAY\s+FROM\s+([^)]+)\s*\)", r"DAY(\1)"),
        ]
        
        for pattern, replacement in replacements:
            raw_response = re.sub(pattern, replacement, raw_response, flags=re.IGNORECASE)
        
        # Fix interval pattern if it still exists
        if "INTERVAL" in raw_response.upper():
            interval_pattern = r"GETDATE\(\)\s*-\s*INTERVAL\s+(\d+)\s+DAY"
            raw_response = re.sub(interval_pattern, r"DATEADD(day, -\1, GETDATE())", raw_response, flags=re.IGNORECASE)
        
        print(f"Final SQL: {raw_response}")
        return raw_response
        
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Ollama API Error: {str(e)}")

# Function to execute SQL query safely
def execute_sql_query(db: Session, sql_query: str):
    try:
        # Create a text object from the SQL string
        sql = text(sql_query)
        
        # Execute the query safely
        result = db.execute(sql)
        return result.all()  # Use .all() for consistency
    except Exception as e:
        db.rollback()  # Rollback in case of failure
        # Print the error to server logs
        print(f"SQL Execution Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"SQL Execution Error: {str(e)}")

# Function to convert SQL result to natural language using SmolLM2
def convert_to_natural_language(sql_result: str) -> str:
    payload = {
        "model": MODEL_NAME,
        "prompt": f"Convert the following SQL output into a natural language response: {sql_result}",
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
            sql_result = result.all()  # Use .all() for consistency
            
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