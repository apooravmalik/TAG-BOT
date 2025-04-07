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

# Extract table metadata and create embeddings
table_names = []
table_descriptions = []

for table in schema_data:
    table_name = table["table_name"]
    column_details = ", ".join(
        [f"{col['name']} ({col['type']})" for col in table["columns"]]
    )
    description = f"Table: {table_name}. Columns: {column_details}."
    
    table_names.append(table_name)
    table_descriptions.append(description)

# Generate embeddings
embeddings = model.encode(table_descriptions, convert_to_numpy=True)

# Create FAISS index
embedding_dim = embeddings.shape[1]
faiss_index = faiss.IndexFlatL2(embedding_dim)
faiss_index.add(embeddings)

# Save FAISS index
FAISS_INDEX_FILE = os.path.join(os.path.dirname(__file__), "faiss_index.bin")
faiss.write_index(faiss_index, FAISS_INDEX_FILE)

print(f"FAISS index saved at: {FAISS_INDEX_FILE}")