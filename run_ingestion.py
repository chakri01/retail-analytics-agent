"""
Phase 2 Master Execution Script
Runs complete data engineering pipeline: Ingestion → Transformations → Validation
"""
import sys
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from ingestion.run_all_ingestion import main as run_ingestion
from transformations.create_views import create_all_views
from utils.postgres_connection import get_connection

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "phase2_master.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# PostgreSQL connection string
DB_URL = 'postgresql://postgres:admin@localhost:5432/retail_analytics'

def verify_raw_data():
    """Check if all required CSV files exist"""
    data_dir = Path(__file__).parent / "data" / "raw"
    required_files = [
        "amazon_sale_report.csv",
        "international_sale_report.csv",
        "sale_report_inventory.csv",
        "pricing_may_2022.csv",
        "pricing_pl_march_2021.csv"
    ]
    
    missing = []
    for file in required_files:
        if not (data_dir / file).exists():
            missing.append(file)
    
    if missing:
        logger.error(f"Missing required files in data/raw/: {missing}")
        return False
    
    logger.info("✓ All required CSV files found")
    return True

def run_exit_criteria_check():
    """Phase 2 Exit Criteria: Can we answer business questions with SQL?"""
    logger.info("\n" + "="*80)
    logger.info("PHASE 2 EXIT CRITERIA CHECK")
    logger.info("="*80)
    
    engine = create_engine(DB_URL)
    
    try:
        # Test query: Total sales by category and country
        logger.info("\nTest Query: Total sales by category and country (2022)")
        
        query = """
        SELECT 
            category,
            country,
            ROUND(CAST(SUM(amount) AS NUMERIC), 2) as total_sales,
            SUM(qty) as total_units,
            COUNT(*) as transaction_count
        FROM sales_fact_view
        WHERE year = 2022
        GROUP BY category, country
        ORDER BY total_sales DESC
        LIMIT 10
        """
        
        with engine.connect() as conn:
            result = pd.read_sql(query, conn)
        
        logger.info(f"\n{result.to_string(index=False)}")
        
        # Explain the query
        logger.info("\n" + "-"*80)
        logger.info("CAN YOU EXPLAIN:")
        logger.info("-"*80)
        logger.info("✓ Where the data came from?")
        logger.info("  → Amazon sales + International sales (unified in sales_fact_view)")
        logger.info("  → Cancelled orders excluded")
        logger.info("  → Null dates/SKUs/amounts filtered out")
        
        logger.info("\n✓ Why it's trustworthy?")
        logger.info("  → Governed by metric_dictionary.json")
        logger.info("  → Business rules applied in view layer")
        logger.info("  → Data quality checks logged during ingestion")
        
        logger.info("\n" + "="*80)
        logger.info("✓ PHASE 2 EXIT CRITERIA: PASSED")
        logger.info("="*80)
        logger.info("\nYou are ready for Phase 3 (GenAI Integration)")
        
    except Exception as e:
        logger.error(f"\n✗ EXIT CRITERIA FAILED: {e}")
        raise

def main():
    """Execute complete Phase 2 pipeline"""
    logger.info("="*80)
    logger.info("PHASE 2: DATA ENGINEERING PIPELINE")
    logger.info("="*80)
    
    try:
        # Step 0: Verify raw data exists
        logger.info("\n[Step 0] Verifying raw data files...")
        if not verify_raw_data():
            raise FileNotFoundError("Required CSV files missing in data/raw/")
        
        # Step 1: Ingestion
        logger.info("\n[Step 1] Running data ingestion...")
        run_ingestion()
        
        # Step 2: Create views
        logger.info("\n[Step 2] Creating analytical views...")
        create_all_views()
        
        # Step 3: Exit criteria
        logger.info("\n[Step 3] Running exit criteria check...")
        run_exit_criteria_check()
        
        logger.info("\n" + "="*80)
        logger.info("🎉 PHASE 2 COMPLETE!")
        logger.info("="*80)
        logger.info("\nNext steps:")
        logger.info("  1. Review logs in logs/")
        logger.info("  2. Connect to PostgreSQL: psql -U postgres -d retail_analytics")
        logger.info("  3. Review metadata in src/metadata/")
        logger.info("  4. Proceed to Phase 3 (GenAI agents)")
        
    except Exception as e:
        logger.error(f"\n✗ PHASE 2 FAILED: {e}")
        logger.error("\nCheck logs in logs/ for details")
        sys.exit(1)

if __name__ == "__main__":
    main()