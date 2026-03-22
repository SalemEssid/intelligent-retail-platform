import psycopg2
from psycopg2.extras import RealDictCursor
import os

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database="retail_data_platform",
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "salemessid15122003")
    )

def fetch_query(query, params=None):
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute(query, params or ())
            return cursor.fetchall()
        finally:
            cursor.close()
    finally:
        conn.close()
