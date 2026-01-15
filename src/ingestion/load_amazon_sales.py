"""
Amazon Sales Data Ingestion
Loads Amazon sale report.csv into PostgreSQL as amazon_sales_raw
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

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

# Setup logging
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

# PostgreSQL connection string from environment variables
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'retail_analytics')

DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def load_amazon_sales():
    """
    Load Amazon sale report into PostgreSQL as amazon_sales_raw.
    - Preserves all columns
    - Casts Date, Qty, Amount to proper types
    - NO filtering (cancelled orders kept at this stage)
    """
    data_path = Path(__file__).parent.parent.parent / "data" / "raw" / "amazon_sale_report.csv"
    
    if not data_path.exists():
        logger.error(f"File not found: {data_path}")
        raise FileNotFoundError(f"amazon_sale_report.csv not found at {data_path}")
    
    logger.info(f"Loading Amazon sales data from {data_path}")
    
    try:
        # Read CSV into pandas
        logger.info("Reading CSV file...")
        df = pd.read_csv(data_path)
        
        # Normalize all column names: strip, lowercase, replace spaces and dashes with underscores
        df.columns = [col.strip().lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        logger.info(f"Normalized columns: {df.columns.tolist()}")

        # Type conversions
        logger.info("Converting data types...")
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['qty'] = pd.to_numeric(df['qty'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        # Data quality checks BEFORE loading
        null_dates = df['date'].isna().sum()
        null_skus = df['sku'].isna().sum() + (df['sku'] == '').sum()
        null_amounts = df['amount'].isna().sum()
        cancelled_count = df[df['status'].str.lower().str.contains('cancelled', na=False)].shape[0]
        
        logger.info(f"Data Quality Check (before load):")
        logger.info(f"  - Total rows: {len(df):,}")
        logger.info(f"  - Null dates: {null_dates}")
        logger.info(f"  - Null/empty SKUs: {null_skus}")
        logger.info(f"  - Null amounts: {null_amounts}")
        logger.info(f"  - Cancelled orders (will be filtered in views): {cancelled_count}")
        
        # Load into PostgreSQL using SQLAlchemy
        logger.info("Loading data into PostgreSQL...")
        engine = create_engine(DB_URL)
        
        # Drop table if exists
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS amazon_sales_raw"))
            conn.commit()
        
        # Insert data
        df.to_sql('amazon_sales_raw', engine, if_exists='replace', index=False, method='multi')
        
        # Verify load
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM amazon_sales_raw"))
            row_count = result.fetchone()[0]
        
        logger.info(f"✓ Loaded {row_count:,} rows into amazon_sales_raw")
        logger.info("✓ Amazon sales data ingestion complete")
        
    except Exception as e:
        logger.error(f"Error loading Amazon sales data: {e}")
        raise

if __name__ == "__main__":
    load_amazon_sales()