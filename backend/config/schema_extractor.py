import os
import json
from sqlalchemy import inspect, text
from database import get_db, engine, DB_SCHEMA
import logging
from contextlib import contextmanager


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get absolute path to config directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Directory of schema_extractor.py
OUTPUT_FILE = os.path.join(BASE_DIR, "schema.json")  # Absolute path to schema.json

@contextmanager
def get_db_session():
    """Get database session using the existing get_db function"""
    db_generator = get_db()
    db = next(db_generator)
    try:
        yield db
    finally:
        try:
            next(db_generator)
        except StopIteration:
            pass

def get_table_schema():
    """
    Get schema details for all tables in the database
    """
    schema_data = {}
    
    try:
        # Create inspector
        inspector = inspect(engine)
        
        # Get all tables in the specified schema
        tables = inspector.get_table_names(schema=DB_SCHEMA)
        
        for table_name in tables:
            table_info = {
                "table_name": table_name,
                "columns": [],
                "primary_key": None,
                "foreign_keys": []
            }
            
            # Get columns
            columns = inspector.get_columns(table_name, schema=DB_SCHEMA)
            for column in columns:
                column_info = {
                    "name": column["name"],
                    "type": str(column["type"]),
                    "nullable": "YES" if column.get("nullable", True) else "NO",
                    "description": get_column_description(table_name, column["name"])
                }
                table_info["columns"].append(column_info)
            
            # Get primary key
            pk = inspector.get_pk_constraint(table_name, schema=DB_SCHEMA)
            if pk and pk.get("constrained_columns"):
                table_info["primary_key"] = pk["constrained_columns"][0] if len(pk["constrained_columns"]) == 1 else pk["constrained_columns"]
            
            # Get foreign keys
            fks = inspector.get_foreign_keys(table_name, schema=DB_SCHEMA)
            for fk in fks:
                for i, col in enumerate(fk["constrained_columns"]):
                    fk_info = {
                        "column": col,
                        "references": f"{fk['referred_table']}({fk['referred_columns'][i]})"
                    }
                    table_info["foreign_keys"].append(fk_info)
            
            schema_data[table_name] = table_info
        
        return list(schema_data.values())
    
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        return []

def get_column_description(table_name, column_name):
    """
    Get column description from extended properties if available
    """
    try:
        with get_db_session() as db:
            query = text("""
                SELECT value 
                FROM sys.extended_properties 
                WHERE major_id = OBJECT_ID(:table_name) 
                AND minor_id = (
                    SELECT column_id 
                    FROM sys.columns 
                    WHERE object_id = OBJECT_ID(:table_name) 
                    AND name = :column_name
                ) 
                AND name = 'MS_Description'
            """)
            
            result = db.execute(query, {"table_name": f"{DB_SCHEMA}.{table_name}", "column_name": column_name})
            row = result.fetchone()
            
            if row and row[0]:
                return row[0]
            return ""
    except Exception as e:
        logger.error(f"Error getting column description: {e}")
        return ""

def save_schema_to_json(output_file=OUTPUT_FILE):
    """
    Save the schema data to a JSON file
    """
    schema_data = get_table_schema()
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, indent=4)
        logger.info(f"Schema data saved to {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error saving schema to JSON: {e}")
        return False
    
def get_relevant_schema_from_retriever(query: str, top_k: int = 2) -> list:
    """
    Get relevant schema (tables + important columns) using retriever logic.
    This version avoids overloading the prompt with full schema.
    """
    from ..schema.retriever import retrieve_table, highlight_relevant_columns

    matched_tables = retrieve_table(query, top_k=top_k)

    if not matched_tables:
        return []

    partial_schema = []

    for table in matched_tables:
        table_name = table["table_name"]
        relevant_columns = highlight_relevant_columns(query, table)

        if not relevant_columns:
            continue  # skip if no relevant columns found

        # Filter columns
        filtered_table = {
            **table,
            "columns": [col for col in table["columns"] if col["name"] in relevant_columns]
        }
        partial_schema.append(filtered_table)

    return partial_schema


if __name__ == "__main__":
    save_schema_to_json()
