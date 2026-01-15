"""
Pricing Data Ingestion
Loads pricing files into PostgreSQL as separate reference tables
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
        logging.FileHandler(LOG_DIR / "ingestion.log"),
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

def load_pricing_may_2022():
    """Load pricing_may_2022.csv as reference table"""
    data_path = Path(__file__).parent.parent.parent / "data" / "raw" / "pricing_may_2022.csv"
    
    if not data_path.exists():
        logger.error(f"File not found: {data_path}")
        raise FileNotFoundError(f"pricing_may_2022.csv not found at {data_path}")
    
    logger.info(f"Loading pricing_may_2022 from {data_path}")
    
    try:
        df = pd.read_csv(data_path)
        
        logger.info(f"  Total rows in CSV: {len(df):,}")
        logger.info(f"  Columns: {list(df.columns)}")
        
        engine = create_engine(DB_URL)
        
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS pricing_may_2022_raw"))
            conn.commit()
        
        df.to_sql('pricing_may_2022_raw', engine, if_exists='replace', index=False, method='multi')
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM pricing_may_2022_raw"))
            row_count = result.fetchone()[0]
        
        logger.info(f"✓ Loaded {row_count:,} rows into pricing_may_2022_raw")
        
    except Exception as e:
        logger.error(f"Error loading pricing_may_2022: {e}")
        raise

def load_pricing_pl_march_2021():
    """Load pricing_pl_march_2021.csv as reference table"""
    data_path = Path(__file__).parent.parent.parent / "data" / "raw" / "pricing_pl_march_2021.csv"
    
    if not data_path.exists():
        logger.error(f"File not found: {data_path}")
        raise FileNotFoundError(f"pricing_pl_march_2021.csv not found at {data_path}")
    
    logger.info(f"Loading pricing_pl_march_2021 from {data_path}")
    
    try:
        df = pd.read_csv(data_path)
        
        logger.info(f"  Total rows in CSV: {len(df):,}")
        logger.info(f"  Columns: {list(df.columns)}")
        
        engine = create_engine(DB_URL)
        
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS pricing_pl_march_2021_raw"))
            conn.commit()
        
        df.to_sql('pricing_pl_march_2021_raw', engine, if_exists='replace', index=False, method='multi')
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM pricing_pl_march_2021_raw"))
            row_count = result.fetchone()[0]
        
        logger.info(f"✓ Loaded {row_count:,} rows into pricing_pl_march_2021_raw")
        
    except Exception as e:
        logger.error(f"Error loading pricing_pl_march_2021: {e}")
        raise

def load_all_pricing():
    """Load all pricing files"""
    logger.info("="*60)
    logger.info("Starting Pricing Data Ingestion")
    logger.info("="*60)
    
    load_pricing_may_2022()
    load_pricing_pl_march_2021()
    
    logger.info("✓ All pricing data ingestion complete")

if __name__ == "__main__":
    load_all_pricing()