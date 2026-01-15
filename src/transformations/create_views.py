"""
Create Analytical Views
Executes SQL view definitions in PostgreSQL
"""
import sys
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append(str(Path(__file__).parent.parent))
from utils.postgres_connection import get_connection

LOG_DIR = Path(__file__).parent.parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "transformations.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# PostgreSQL connection string
DB_URL = 'postgresql://postgres:admin@localhost:5432/retail_analytics'

def execute_sql_file(sql_file_path, engine):
    logger.info(f"Executing {sql_file_path.name}...")

    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql))

    logger.info(f"[OK] {sql_file_path.name} executed successfully")

# def execute_sql_file(sql_file_path, engine):
#     """Execute SQL from file"""
#     logger.info(f"Executing {sql_file_path.name}...")
    
#     with open(sql_file_path, 'r') as f:
#         sql = f.read()
    
#     # Split by semicolon and execute each statement
#     statements = [s.strip() for s in sql.split(';') if s.strip() and not s.strip().startswith('--')]
    
#     with engine.connect() as conn:
#         for statement in statements:
#             # Skip comment-only statements
#             if statement.startswith('--') or statement.startswith('/*'):
#                 continue
#             try:
#                 conn.execute(text(statement))
#                 conn.commit()
#             except Exception as e:
#                 logger.error(f"Error in statement: {statement[:100]}...")
#                 raise e
    
#     logger.info(f"✓ {sql_file_path.name} executed successfully")

def create_all_views():
    """Create all analytical views"""
    logger.info("="*80)
    logger.info("PHASE 2: CREATING ANALYTICAL VIEWS")
    logger.info("="*80)
    
    transformations_dir = Path(__file__).parent
    
    views = [
        'sales_fact_view.sql',
        'product_dim_view.sql',
        'inventory_dim_view.sql'
    ]
    engine = create_engine(DB_URL)
    
    try:
        for view_file in views:
            view_path = transformations_dir / view_file
            if view_path.exists():
                execute_sql_file(view_path, engine)
            else:
                logger.warning(f"View file not found: {view_file}")
        
        # Validate views
        logger.info("\n" + "="*80)
        logger.info("VIEW VALIDATION")
        logger.info("="*80)
        
        with engine.connect() as conn:
            # Check sales_fact_view
            logger.info("\n[1] sales_fact_view:")
            result = conn.execute(text("SELECT COUNT(*) FROM sales_fact_view"))
            total_rows = result.fetchone()[0]
            logger.info(f"  Total rows: {total_rows:,}")
            
            channel_breakdown = pd.read_sql("""
                SELECT channel, COUNT(*) as row_count, 
                       ROUND(CAST(SUM(amount) AS NUMERIC), 2) as total_sales 
                FROM sales_fact_view 
                GROUP BY channel
            """, conn)
            logger.info(f"\n{channel_breakdown.to_string(index=False)}")
            
            date_range_result = conn.execute(text("""
                SELECT MIN(order_date) as earliest, MAX(order_date) as latest 
                FROM sales_fact_view
            """))
            date_range = date_range_result.fetchone()
            logger.info(f"\n  Date range: {date_range[0]} to {date_range[1]}")
            
            # Check product_dim_view
            logger.info("\n[2] product_dim_view:")
            result = conn.execute(text("SELECT COUNT(*) FROM product_dim_view"))
            unique_skus = result.fetchone()[0]
            logger.info(f"  Unique SKUs: {unique_skus:,}")
            
            source_breakdown = pd.read_sql("""
                SELECT data_sources, COUNT(*) as sku_count 
                FROM product_dim_view 
                GROUP BY data_sources
            """, conn)
            logger.info(f"\n{source_breakdown.to_string(index=False)}")
            
            # Check inventory_dim_view
            logger.info("\n[3] inventory_dim_view:")
            result = conn.execute(text("SELECT COUNT(*) FROM inventory_dim_view"))
            inventory_skus = result.fetchone()[0]
            logger.info(f"  Inventory SKUs: {inventory_skus:,}")
            
            stock_status = pd.read_sql("""
                SELECT stock_status, COUNT(*) as sku_count, SUM(stock) as total_units
                FROM inventory_dim_view 
                GROUP BY stock_status
                ORDER BY 
                    CASE stock_status
                        WHEN 'Out of Stock' THEN 1
                        WHEN 'Low Stock' THEN 2
                        WHEN 'Medium Stock' THEN 3
                        WHEN 'High Stock' THEN 4
                    END
            """, conn)
            logger.info(f"\n{stock_status.to_string(index=False)}")
        
        logger.info("\n" + "="*80)
        logger.info("✓ ALL VIEWS CREATED AND VALIDATED SUCCESSFULLY")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Error creating views: {e}")
        raise

if __name__ == "__main__":
    create_all_views()