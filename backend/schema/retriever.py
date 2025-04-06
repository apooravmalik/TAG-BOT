import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import json
import os

# Paths
FAISS_INDEX_FILE = os.path.join(os.path.dirname(__file__), "faiss_index.bin")
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "schema.json")

# Load model, schema, and FAISS index
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
with open(SCHEMA_FILE, "r") as f:
    schema_data = json.load(f)
faiss_index = faiss.read_index(FAISS_INDEX_FILE)

# Step 2: Retrieve the most relevant table based on a query
def retrieve_table(query, top_k=1):
    query_embedding = model.encode([query], convert_to_numpy=True)
    distances, indices = faiss_index.search(query_embedding, top_k)

    results = []
    for i in range(top_k):
        if indices[0][i] < len(schema_data):
            results.append(schema_data[indices[0][i]])
    return results  # Return full schema, not just table name

# Step 3: Highlight relevant columns from the table
def highlight_relevant_columns(query, table_schema, threshold=0.1):
    """
    Returns a list of column names that are semantically similar to the query.
    Works with real schema.json structure.
    """
    query_embedding = model.encode([query], convert_to_numpy=True)[0]

    column_scores = []
    for column in table_schema["columns"]:
        col_name = column["name"]
        col_type = column.get("type", "")

        col_text = f"{col_name.replace('_', ' ')} is of type {col_type}"
        col_embedding = model.encode([col_text], convert_to_numpy=True)[0]

        # Cosine similarity
        score = np.dot(query_embedding, col_embedding) / (
            np.linalg.norm(query_embedding) * np.linalg.norm(col_embedding)
        )
        column_scores.append((col_name, score))

    highlighted_columns = [col for col, score in column_scores if score >= threshold]
    return highlighted_columns


# Test retrieval and column highlighting
if __name__ == "__main__":
    query = input("Enter your search query: ")
    
    # Step 2: Retrieve table
    matched_tables = retrieve_table(query)
    if not matched_tables:
        print("No relevant table found.")
    else:
        selected_table = matched_tables[0]
        print("Relevant Table:", selected_table["table_name"])
        
        # Step 3: Highlight columns
        highlighted = highlight_relevant_columns(query, selected_table)
        print("Highlighted Columns:", highlighted)
