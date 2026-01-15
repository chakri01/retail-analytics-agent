"""
Master Ingestion Script
Runs all data ingestion in sequence
"""
import sys
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

sys.path.append(str(Path(__file__).parent.parent))

from ingestion.load_amazon_sales import load_amazon_sales
from ingestion.load_international_sales import load_international_sales
from ingestion.load_inventory import load_inventory
from ingestion.load_pricing import load_all_pricing

LOG_DIR = Path(__file__).parent.parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# PostgreSQL connection string from environment variables
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'retail_analytics')

DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def main():
    """Run all ingestion scripts"""
    logger.info("="*80)
    logger.info("PHASE 2: DATA INGESTION - STARTING")
    logger.info("="*80)
    
    try:
        # 1. Amazon Sales
        logger.info("\n[1/4] Loading Amazon Sales...")
        load_amazon_sales()
        
        # 2. International Sales
        logger.info("\n[2/4] Loading International Sales...")
        load_international_sales()
        
        # 3. Inventory
        logger.info("\n[3/4] Loading Inventory...")
        load_inventory()
        
        # 4. Pricing
        logger.info("\n[4/4] Loading Pricing Data...")
        load_all_pricing()
        
        # Summary
        logger.info("\n" + "="*80)
        logger.info("INGESTION COMPLETE - TABLE SUMMARY")
        logger.info("="*80)
        
        # Get table list from PostgreSQL
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            tables_query = """
                SELECT table_name as name
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """
            tables = pd.read_sql(tables_query, conn)
        
        logger.info(f"\n{tables.to_string(index=False)}")
        
        # Row counts
        logger.info("\nRow Counts:")
        with engine.connect() as conn:
            for table_name in tables['name']:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.fetchone()[0]
                logger.info(f"  {table_name}: {count:,} rows")
        
        logger.info("\n✓ ALL INGESTION COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        logger.error(f"\n✗ INGESTION FAILED: {e}")
        raise

if __name__ == "__main__":
    main()