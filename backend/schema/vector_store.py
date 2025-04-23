import json
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import os

# Load schema
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "schema.json")

# Load pre-trained model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# Load schema data
with open(SCHEMA_FILE, "r") as f:
    schema_data = json.load(f)

# Extract table and column metadata and create embeddings
table_names = []
table_descriptions = []
metadata = []
embedding_id = 0

for table in schema_data:
    table_name = table["table_name"]
    
    # Table-level description
    column_details = ", ".join(
        [f"{col['name']} ({col['type']})" for col in table["columns"]]
    )
    fk_details = ""
    if "foreign_keys" in table and table["foreign_keys"]:
        fk_descriptions = [
            f"{fk['column']} references {fk['references']}" for fk in table["foreign_keys"]
        ]
        fk_details = " Foreign Keys: " + "; ".join(fk_descriptions)
    
    table_description = f"Table: {table_name}. Columns: {column_details}.{fk_details}"
    
    # Add table metadata
    metadata.append({
        "embedding_id": embedding_id,
        "table": table_name,
        "column": None,
        "description": table_description
    })
    table_names.append(table_name)
    table_descriptions.append(table_description)
    embedding_id += 1
    
    # Column-level descriptions
    for col in table["columns"]:
        col_name = col["name"]
        col_type = col["type"]
        col_description = f"{table_name}.{col_name} ({col_type})"
        
        # Add column metadata
        metadata.append({
            "embedding_id": embedding_id,
            "table": table_name,
            "column": col_name,
            "description": col_description
        })
        table_names.append(table_name)
        table_descriptions.append(col_description)
        embedding_id += 1

# Generate embeddings
embeddings = model.encode(table_descriptions, convert_to_numpy=True)

# Create FAISS index
embedding_dim = embeddings.shape[1]
faiss_index = faiss.IndexFlatL2(embedding_dim)
faiss_index.add(embeddings)

# Save FAISS index
FAISS_INDEX_FILE = os.path.join(os.path.dirname(__file__), "faiss_index.bin")
faiss.write_index(faiss_index, FAISS_INDEX_FILE)

# Save metadata
METADATA_FILE = os.path.join(os.path.dirname(__file__), "faiss_metadata.json")
with open(METADATA_FILE, "w") as f:
    json.dump(metadata, f)

print(f"✅ FAISS index saved at: {FAISS_INDEX_FILE}")
print(f"✅ Metadata saved at: {METADATA_FILE}")