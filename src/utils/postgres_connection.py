"""
PostgreSQL Connection Manager
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration from environment variables
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'retail_analytics'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'admin'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

def get_connection():
    """Get PostgreSQL connection"""
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def execute_query(query, conn=None, fetch=True):
    """Execute query and return results"""
    should_close = False
    if conn is None:
        conn = get_connection()
        should_close = True
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        
        if fetch:
            result = cursor.fetchall()
            return result
        else:
            conn.commit()
            return None
    finally:
        if should_close:
            conn.close()

def get_table_list(schema='analytics', conn=None):
    """Get list of tables/views in schema"""
    should_close = False
    if conn is None:
        conn = get_connection()
        should_close = True
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}'
        """)
        return cursor.fetchall()
    finally:
        if should_close:
            conn.close()