import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import json
import os

# Load FAISS index
FAISS_INDEX_FILE = os.path.join(os.path.dirname(__file__), "faiss_index.bin")
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "schema.json")

# Load Sentence Transformer model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# Load schema data
with open(SCHEMA_FILE, "r") as f:
    schema_data = json.load(f)

# Load FAISS index
faiss_index = faiss.read_index(FAISS_INDEX_FILE)

# Retrieve the most relevant table based on a query
def retrieve_table(query, top_k=1):
    query_embedding = model.encode([query], convert_to_numpy=True)
    
    # Search FAISS index
    distances, indices = faiss_index.search(query_embedding, top_k)

    results = []
    for i in range(top_k):
        if indices[0][i] < len(schema_data):
            results.append(schema_data[indices[0][i]]["table_name"])

    return results

# Test retrieval
if __name__ == "__main__":
    query = input("Enter your search query: ")
    results = retrieve_table(query)
    print("Relevant Table(s):", results)
