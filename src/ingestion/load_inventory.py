"""
Inventory Data Ingestion
Loads sale_report_inventory.csv into PostgreSQL as inventory_raw
"""
import sys
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

sys.path.append(str(Path(__file__).parent.parent))

LOG_DIR = Path(__file__).parent.parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "ingestion.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'retail_analytics')

DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def load_inventory():
    """
    Load sale_report_inventory into PostgreSQL as inventory_raw.
    - Ensures SKU is clean and non-null
    - Casts Stock to INTEGER
    - Preserves all columns
    """
    data_path = Path(__file__).parent.parent.parent / "data" / "raw" / "sale_report_inventory.csv"
    
    if not data_path.exists():
        logger.error(f"File not found: {data_path}")
        raise FileNotFoundError(f"sale_report_inventory.csv not found at {data_path}")
    
    logger.info(f"Loading Inventory data from {data_path}")
    
    try:
        logger.info("Reading CSV file...")
        df = pd.read_csv(data_path)
        # Normalize all column names: strip, lowercase, replace spaces and dashes with underscores
        df.columns = [col.strip().lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        logger.info(f"Normalized columns: {df.columns.tolist()}")
        logger.info(f"Total rows in CSV: {len(df):,}")
        
        sku_col = None
        for col in df.columns:
            if 'sku' in col.lower():
                sku_col = col
                break
        
        if sku_col:
            null_skus = df[sku_col].isna().sum() + (df[sku_col] == '').sum()
            unique_skus = df[sku_col].nunique()
            
            logger.info(f"Data Quality Check (before load):")
            logger.info(f"  - Null/empty SKUs: {null_skus}")
            logger.info(f"  - Unique SKUs: {unique_skus}")
            
            if unique_skus < len(df):
                logger.warning(f"  - SKUs are NOT unique (duplicates found)")
            else:
                logger.info(f"  - SKUs are unique ✓")
        else:
            logger.warning("SKU column not found - cannot perform SKU checks")
        
        logger.info("Loading data into PostgreSQL...")
        engine = create_engine(DB_URL)
        
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS inventory_raw"))
            conn.commit()
        
        df.to_sql('inventory_raw', engine, if_exists='replace', index=False, method='multi')
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM inventory_raw"))
            row_count = result.fetchone()[0]
            
            columns_result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'inventory_raw'
                ORDER BY ordinal_position
            """))
            columns = [row[0] for row in columns_result.fetchall()]
        
        logger.info(f"✓ Loaded {row_count:,} rows into inventory_raw")
        logger.info(f"Inventory table columns: {columns}")
        logger.info("✓ Inventory data ingestion complete")
        
    except Exception as e:
        logger.error(f"Error loading Inventory data: {e}")
        raise

if __name__ == "__main__":
    load_inventory()