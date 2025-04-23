import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import json
import os
import re

# Paths
FAISS_INDEX_FILE = os.path.join(os.path.dirname(__file__), "faiss_index.bin")
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "schema.json")

# Load model, schema, and FAISS index
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
with open(SCHEMA_FILE, "r") as f:
    schema_data = json.load(f)
faiss_index = faiss.read_index(FAISS_INDEX_FILE)

# Modified retrieve_table function with priority for IncidentLog_TBL
def retrieve_table(query, top_k=5):
    query_embedding = model.encode([query], convert_to_numpy=True)
    distances, indices = faiss_index.search(query_embedding, top_k)
    
    # Get all potential results
    all_results = []
    for i in range(top_k):
        if indices[0][i] < len(schema_data):
            all_results.append(schema_data[indices[0][i]])
    
    # Separate IncidentLog_TBL from other tables
    incident_log_results = []
    other_results = []
    
    for result in all_results:
        if result["table_name"] == "IncidentLog_TBL":
            incident_log_results.append(result)
        else:
            other_results.append(result)
    
    # Prioritize IncidentLog_TBL results, then append other results
    prioritized_results = incident_log_results + other_results
    
    return prioritized_results[:top_k]

# Detect key query aspects
def detect_query_aspects(query):
    """Detect the different aspects of a query to determine relevant columns"""
    query_lower = query.lower()
    
    aspects = {
        "count": any(word in query_lower for word in ["count", "number", "total", "sum", "how many", "most", "least"]),
        "category": any(word in query_lower for word in ["category", "type", "classification", "kind"]),
        "status": any(word in query_lower for word in ["status", "state", "open", "closed", "pending", "active", "resolved"]),
        "location": any(word in query_lower for word in ["location", "where", "place", "building", "site", "address", "zone", "area"]),
        "time": any(word in query_lower for word in ["time", "date", "when", "period", "during", "recent", "latest", "oldest"]),
        "list": any(word in query_lower for word in ["list", "show", "display", "get", "provide", "find"]),
    }
    
    return aspects

# Improved highlight_relevant_columns function with better query aspect detection
def highlight_relevant_columns(query, table_schema, max_columns=8):
    """
    Returns columns that are relevant to specific query aspects.
    """
    aspects = detect_query_aspects(query)
    table_name = table_schema["table_name"]
    
    selected_columns = []
    
    # Always include the primary key
    for column in table_schema["columns"]:
        if column["name"].endswith("_PRK"):
            selected_columns.append(column["name"])
            break
    
    # Table-specific selection based on query aspects
    if table_name == "IncidentLog_TBL":
        # For status-related queries
        if aspects["status"]:
            selected_columns.append("inlStatus_FRK")
        
        # For location-related queries
        if aspects["location"]:
            location_columns = [col["name"] for col in table_schema["columns"] if any(
                loc_term in col["name"].lower() for loc_term in 
                ["building", "zone", "street", "location", "address", "area", "site", "map"]
            )]
            selected_columns.extend(location_columns[:3])  # Limit to 3 location columns
        
        # For category-related queries
        if aspects["category"]:
            selected_columns.append("inlCategory_FRK")
            if "subcategory" in query.lower():
                selected_columns.append("inlSubCategory_FRK")
        
        # For time-related queries
        if aspects["time"]:
            time_columns = [col["name"] for col in table_schema["columns"] if "time" in col["name"].lower() or "date" in col["name"].lower()]
            selected_columns.extend(time_columns[:2])  # Limit to 2 time columns
            
        # For counting/aggregation queries
        if aspects["count"]:
            if "building" in query.lower():
                selected_columns.append("inlBuilding_FRK")
            if "status" in query.lower():
                selected_columns.append("inlStatus_FRK")
            if "category" in query.lower():
                selected_columns.append("inlCategory_FRK")
                
    elif table_name == "IncidentStatus_TBL":
        # For status tables, add the name/description field
        status_name_columns = [col["name"] for col in table_schema["columns"] if any(
            name_term in col["name"].lower() for name_term in ["name", "desc", "title", "text"]
        )]
        selected_columns.extend(status_name_columns[:1])  # Add just the main name field
        
    elif table_name == "Building_TBL":
        # For building tables, add name and address fields
        building_info_columns = [col["name"] for col in table_schema["columns"] if any(
            term in col["name"].lower() for term in ["name", "address", "location"]
        )]
        selected_columns.extend(building_info_columns[:2])  # Limit to 2 building info columns
        
    elif table_name == "IncidentCategory_TBL":
        # For category tables, add name field
        category_name_columns = [col["name"] for col in table_schema["columns"] if "name" in col["name"].lower()]
        selected_columns.extend(category_name_columns[:1])  # Add just the main name field
    
    # Ensure we don't have duplicates
    selected_columns = list(dict.fromkeys(selected_columns))
    
    # Limit to max_columns
    return selected_columns[:max_columns]

# Main execution
if __name__ == "__main__":
    query = input("Enter your search query: ")
    
    matched_tables = retrieve_table(query)
    
    if not matched_tables:
        print("No relevant table found.")
    else:
        combined_tables = []
        all_highlighted_columns = {}
        
        for table in matched_tables:
            table_name = table["table_name"]
            combined_tables.append(table_name)

            highlighted = highlight_relevant_columns(query, table)
            if highlighted:
                all_highlighted_columns[table_name] = highlighted

        # Deduplicate table names while preserving order
        seen = set()
        combined_tables = [x for x in combined_tables if not (x in seen or seen.add(x))]

        # Output
        print("Relevant Tables:", combined_tables)
        print("Highlighted Columns:")
        for table_name, columns in all_highlighted_columns.items():
            print(f"  {table_name}: {columns}")