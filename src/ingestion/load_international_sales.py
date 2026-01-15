"""
International Sales Data Ingestion
Loads International sale report.csv into PostgreSQL as international_sales_raw
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

def load_international_sales():
    """
    Load International sale report into PostgreSQL as international_sales_raw.
    - Normalizes uppercase column names to snake_case
    - Casts DATE, PCS, GROSS AMT to proper types
    - Preserves all columns
    """
    data_path = Path(__file__).parent.parent.parent / "data" / "raw" / "international_sale_report.csv"
    
    if not data_path.exists():
        logger.error(f"File not found: {data_path}")
        raise FileNotFoundError(f"International sale report.csv not found at {data_path}")
    
    logger.info(f"Loading International sales data from {data_path}")
    
    try:
        # Read CSV into pandas
        logger.info("Reading CSV file...")
        df = pd.read_csv(data_path)
        
        # Rename columns (normalize)
        df = df.rename(columns={
            "DATE": "date",
            "Months": "months",
            "CUSTOMER": "customer",
            "Style": "style",
            "SKU": "sku",
            "Size": "size",
            "PCS": "pcs",
            "RATE": "rate",
            "GROSS AMT": "gross_amt"
        })
        
        # Type conversions
        logger.info("Converting data types...")
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['pcs'] = pd.to_numeric(df['pcs'], errors='coerce')
        df['rate'] = pd.to_numeric(df['rate'], errors='coerce')
        df['gross_amt'] = pd.to_numeric(df['gross_amt'], errors='coerce')
        
        # Data quality checks BEFORE loading
        null_dates = df['date'].isna().sum()
        null_skus = df['sku'].isna().sum() + (df['sku'] == '').sum()
        null_pcs = df['pcs'].isna().sum()
        null_gross_amt = df['gross_amt'].isna().sum()
        
        # Check negative values
        negative_pcs = (df['pcs'] < 0).sum()
        negative_amt = (df['gross_amt'] < 0).sum()
        
        logger.info(f"Data Quality Check (before load):")
        logger.info(f"  - Total rows: {len(df):,}")
        logger.info(f"  - Null dates: {null_dates}")
        logger.info(f"  - Null/empty SKUs: {null_skus}")
        logger.info(f"  - Null PCS: {null_pcs}")
        logger.info(f"  - Null gross amounts: {null_gross_amt}")
        
        if negative_pcs > 0:
            logger.warning(f"  - Negative PCS values found: {negative_pcs}")
        if negative_amt > 0:
            logger.warning(f"  - Negative amounts found: {negative_amt}")
        
        # Load into PostgreSQL using SQLAlchemy
        logger.info("Loading data into PostgreSQL...")
        engine = create_engine(DB_URL)
        
        # Drop table if exists
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS international_sales_raw"))
            conn.commit()
        
        # Insert data
        df.to_sql('international_sales_raw', engine, if_exists='replace', index=False, method='multi')
        
        # Verify load
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM international_sales_raw"))
            row_count = result.fetchone()[0]
        
        logger.info(f"✓ Loaded {row_count:,} rows into international_sales_raw")
        logger.info("✓ International sales data ingestion complete")
        
    except Exception as e:
        logger.error(f"Error loading International sales data: {e}")
        raise

if __name__ == "__main__":
    load_international_sales()