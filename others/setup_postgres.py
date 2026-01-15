"""
PostgreSQL Database & Schema Setup
Run this ONCE before Phase 2
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os

load_dotenv()

# Get credentials from .env file
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'retail_analytics')

# Initial connection config
ADMIN_CONFIG = {
    'dbname': 'postgres',  # Connect to default postgres DB first
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'port': DB_PORT
}

def setup_database():
    """Create database and schemas"""
    
    # Connect to default postgres database
    conn = psycopg2.connect(**ADMIN_CONFIG)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # 1. Create database
    print(f"Creating database: {DB_NAME}...")
    try:
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        print("✓ Database created")
    except psycopg2.errors.DuplicateDatabase:
        print("⚠ Database already exists")
    
    cursor.close()
    conn.close()
    
    # 2. Connect to new database and create schemas
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    
    schemas = ['raw', 'analytics', 'metadata']
    
    for schema in schemas:
        print(f"Creating schema: {schema}...")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        print(f"✓ Schema {schema} created")
    
    conn.commit()
    
    # 3. Verify
    print("\n" + "="*80)
    print("VERIFICATION")
    print("="*80)
    cursor.execute("""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name IN ('raw', 'analytics', 'metadata')
    """)
    schemas_found = cursor.fetchall()
    print(f"Schemas found: {[s[0] for s in schemas_found]}")
    
    cursor.close()
    conn.close()
    
    print("\n✓ PostgreSQL setup complete!")

if __name__ == "__main__":
    setup_database()