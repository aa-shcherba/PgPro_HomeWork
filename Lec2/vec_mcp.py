from mcp.server.fastmcp import FastMCP
import os
import csv
import re
import psycopg
from psycopg.rows import dict_row # Используем dict_row для удобного вывода
import numpy as np
from sentence_transformers import SentenceTransformer


PG_USER = "admin"
PG_PASS = "secret"
PG_DB   = "testdb"
PG_HOST = "localhost"
PG_PORT = 5431

MODEL_NAME = "all-MiniLM-L6-v2"
EMB_DIM = 384
model = SentenceTransformer(MODEL_NAME)

mcp = FastMCP(f"Vector Search Connector for docs database (Model: {MODEL_NAME}, Dim: {EMB_DIM})")

def _conn():
    """Создает новое синхронное соединение psycopg."""
    return psycopg.connect(
        user=PG_USER, 
        password=PG_PASS, 
        dbname=PG_DB, 
        host=PG_HOST, 
        port=PG_PORT,
        row_factory=dict_row
    )

@mcp.tool()
def search_vector(query: str, k: int = 5) -> list[dict]:
    """
    Performs semantic vector search on the 'docs' database table 
    using the provided query and returns the top k results.
    
    The query is first encoded using the SentenceTransformer model (all-MiniLM-L6-v2).

    Args:
        query: The natural language search query.
        k: The number of top documents to return (default is 5).

    Returns:
        A list of dictionaries, each containing id, score, and snippet.
    """
    print(f"Executing vector search for query: '{query}'")

    query_vec_encoded = model.encode(
        [query], 
        normalize_embeddings=True
    ).astype(np.float32)[0].tolist()

    sql = """
        SELECT 
            id, 
            1 - (emb <=> %s::vector) AS score,
            substring(text for 500) AS snippet
        FROM docs
        ORDER BY emb <=> %s::vector
        LIMIT %s
    """

    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (query_vec_encoded, query_vec_encoded, k))
            results = cur.fetchall()
            return results

if __name__ == "__main__":
    mcp.run(transport="streamable-http")