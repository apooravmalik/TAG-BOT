from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import requests
from sqlalchemy.orm import Session
from config.database import get_db
from sqlalchemy import text
import re
from schema.schema_extractor import get_relevant_schema_from_retriever

app = FastAPI()

# Ollama API Configuration
OLLAMA_URL = "http://127.0.0.1:11500/api/generate"
MODEL_NAME = "sql-smol"

# Request Model
class QueryRequest(BaseModel):
    query: str

# SQL Standardization Function
def standardize_sql(sql_query):
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

    column_replacements = {
        r'\bLastUpdateDate\b': 'updated_at',
        r'\bCreated\b': 'created_at',
        r'\bUpdatedAt\b': 'updated_at',
        r'\bCreatedAt\b': 'created_at',
        r'\blast_login\b': 'last_login_at',
        r'\blast_updated\b': 'updated_at',
        r'\bdate_created_at\b': 'created_at',
        r'\btextMME\b': 'text_mme',
    }

    syntax_replacements = {
        r'CURRENT_DATE - INTERVAL \'(\d+)\' DAY': r'DATEADD(day, -\1, GETDATE())',
        r'DATE\([\w_]+\) > CURRENT_DATE - INTERVAL': r'created_at >= DATEADD(day, -',
        r'NOW\(\)': 'GETDATE()',
        r'CURRENT_DATE': 'CAST(GETDATE() AS DATE)',
    }

    for pattern, replacement in table_replacements.items():
        sql_query = re.sub(r'FROM\s+' + pattern, f'FROM {replacement}', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'JOIN\s+' + pattern, f'JOIN {replacement}', sql_query, flags=re.IGNORECASE)

    for pattern, replacement in column_replacements.items():
        sql_query = re.sub(pattern, replacement, sql_query, flags=re.IGNORECASE)

    for pattern, replacement in syntax_replacements.items():
        sql_query = re.sub(pattern, replacement, sql_query)

    if not sql_query.strip().endswith(';'):
        sql_query = sql_query.strip() + ';'

    return sql_query

# Function to fetch table schema
def get_table_schema(db: Session, table_name: str) -> dict:
    try:
        query = text("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = :table_name
        """)
        result = db.execute(query, {"table_name": table_name}).all()

        if not result:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        return {
            "table_name": table_name,
            "columns": [
                {"name": row.COLUMN_NAME, "type": row.DATA_TYPE, "nullable": row.IS_NULLABLE}
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching schema: {str(e)}")

# Function to generate SQL from natural language
def generate_sql(natural_query: str, schema: dict) -> str:
    table_name = schema["table_name"]
    columns_info = "\n".join([
        f"- {col['name']} ({col['type']}, {'NULL' if col['nullable'] == 'YES' else 'NOT NULL'})"
        for col in schema["columns"]
    ])

    schema_prompt = f"""
    Table: {table_name}
    Columns:
    {columns_info}
    """

    payload = {
        "model": MODEL_NAME,
        "prompt": f"""You are a SQL code generator. Output ONLY valid SQL code with no explanations.

"Request: Convert this into MSSQL. Prefer to show readable question and answer texts (e.g., `text_mme`) instead of just showing IDs. Return useful and user-friendly information for the query: {natural_query}"


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

        if "```" in raw_response:
            sql_match = re.search(r"```(?:sql)?\s*(.*?)\s*```", raw_response, re.DOTALL)
            if sql_match:
                raw_response = sql_match.group(1).strip()

        return standardize_sql(raw_response)
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Ollama API Error: {str(e)}")

@app.get("/schema/{table_name}")
async def fetch_schema(table_name: str, db: Session = Depends(get_db)):
    return get_table_schema(db, table_name)

@app.post("/query")
async def process_query(request: QueryRequest, db: Session = Depends(get_db)):
    try:
        relevant_schemas = get_relevant_schema_from_retriever(request.query)
        print(f"DEBUG: Retrieved {len(relevant_schemas)} relevant schema(s).")

        if not relevant_schemas:
            raise HTTPException(status_code=404, detail="No relevant schemas found.")

        all_results = []
        all_sql = []

        for schema in relevant_schemas:
            try:
                sql_query_raw = generate_sql(request.query, schema)
                sql_query = sql_query_raw.strip().split(";")[0] + ";"

                print(f"Executing SQL for table `{schema['table_name']}`:\n{sql_query}")

                if not sql_query or "no sql" in sql_query.lower():
                    continue

                result = db.execute(text(sql_query))
                sql_result = result.all()

                if sql_result:
                    formatted = [dict(row._mapping) for row in sql_result]
                    all_results.extend(formatted)
                    all_sql.append(sql_query)
            except Exception as e:
                print(f"⚠️ Error with table `{schema['table_name']}`: {e}")
                continue

        if not all_results:
            return {
                "query": request.query,
                "sql": all_sql,
                "response": "No results found for your query."
            }

        explanation_payload = {
            "model": MODEL_NAME,
            "prompt": f"Summarize the following SQL results in one line: {str(all_results)}",
            "stream": False
        }

        try:
            explanation_response = requests.post(OLLAMA_URL, json=explanation_payload)
            explanation_response.raise_for_status()
            explanation = explanation_response.json().get("response", "").strip()
        except requests.RequestException:
            explanation = "Unable to generate a summary."

        return {
            "query": request.query,
            "sql": all_sql,
            "data": all_results,
            "explanation": explanation
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {str(e)}")
