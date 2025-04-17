import json
from sqlalchemy import inspect
from database import engine, test_connection

def extract_table_schema(table_name, schema="dbo"):
    """
    Extract schema information for the specified table and save to JSON
    
    Args:
        table_name (str): Name of the table
        schema (str): Database schema name (default: dbo)
    """
    # Test database connection
    if not test_connection():
        print("Failed to connect to database. Please check your credentials.")
        return
    
    try:
        # Create an inspector
        inspector = inspect(engine)
        
        # Check if table exists
        if table_name not in inspector.get_table_names(schema=schema):
            print(f"Table '{table_name}' not found in schema '{schema}'")
            return
        
        # Get column information
        columns = inspector.get_columns(table_name, schema=schema)
        column_info = []
        
        for column in columns:
            column_data = {
                "name": column["name"],
                "type": str(column["type"]),
                "nullable": column["nullable"],
                "default": str(column["default"]) if column["default"] else None,
                "primary_key": column.get("primary_key", False)
            }
            column_info.append(column_data)
        
        # Get primary key info
        pk_info = inspector.get_pk_constraint(table_name, schema=schema)
        
        # Get foreign key info
        fk_info = inspector.get_foreign_keys(table_name, schema=schema)
        
        # Get index info
        index_info = inspector.get_indexes(table_name, schema=schema)
        
        # Compile complete schema information
        schema_info = {
            "table_name": table_name,
            "schema": schema,
            "columns": column_info,
            "primary_key_constraint": pk_info,
            "foreign_keys": fk_info,
            "indexes": index_info
        }
        
        # Save to JSON file
        output_file = f"{table_name}_schema.json"
        with open(output_file, 'w') as f:
            json.dump(schema_info, f, indent=4)
        
        print(f"Schema for '{table_name}' saved to {output_file}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

# Simply modify this line to extract schema for your desired table
table_name = "IncidentSubSubCategory_TBL"  # <-- Change this to your table name

# Execute the extraction
if __name__ == "__main__":
    extract_table_schema(table_name)