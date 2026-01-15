# Retail Insights Assistant - Phase 2: Data Engineering

## Overview
Phase 2 establishes a **clean, queryable analytical foundation** using DuckDB. This layer is **LLM-independent** and serves as the governed data source for Phase 3 (GenAI agents).

---

## What Gets Ingested

### 1. Amazon Sales (`Amazon sale report.csv`)
- **~129,000 rows**
- Order-level sales data from Amazon.in
- **Key fields**: Order ID, Date, SKU, Category, Qty, Amount, Status, Ship details
- **Loaded as**: `amazon_sales_raw`

### 2. International Sales (`International sale report.csv`)
- **~18,500 rows**
- International B2B sales transactions
- **Key fields**: Date, Customer, SKU, Size, PCS (quantity), Gross Amount
- **Loaded as**: `international_sales_raw`

### 3. Inventory (`sale_report_inventory.csv`)
- **~1,200 rows**
- Current stock levels by SKU
- **Key fields**: SKU, Category, Size, Stock
- **Loaded as**: `inventory_raw`

### 4. Pricing Data
- `pricing_may_2022.csv` → `pricing_may_2022_raw`
- `pricing_pl_march_2021.csv` → `pricing_pl_march_2021_raw`
- **Status**: Loaded as reference tables, not yet integrated into analytical views

---

## What Is Excluded (Explicitly)

### ❌ Cancelled Orders
- **Rationale**: Business logic decision. Cancelled orders do not contribute to revenue or operational KPIs.
- **Implementation**: Filtered in `sales_fact_view` using `WHERE LOWER(status) NOT LIKE '%cancelled%'`
- **Impact**: ~X% of Amazon raw data excluded (logged during ingestion)

### ❌ Null Data Quality Issues
- Rows with `NULL` or empty:
  - `order_date`
  - `sku`
  - `amount` / `gross_amt`
- **Rationale**: Cannot perform meaningful analysis without these core fields
- **Implementation**: Filtered in analytical views

---

## Canonical Views (The Core Intelligence Layer)

### 1️⃣ `sales_fact_view`
**Purpose**: Unified sales transactions across channels

**Grain**: One row = one SKU sold in an order/transaction

**Unifies**:
- Amazon sales (domestic)
- International sales

**Standardized Columns**:
```
order_id, order_date, sku, category, size, style, qty, amount,
channel, country, state, city, currency, year, month, quarter, month_name
```

**Key Transformations**:
- `Status != 'Cancelled'` → excluded
- `Date` → `order_date` (DATE type)
- `Qty` / `PCS` → `qty` (INTEGER)
- `Amount` / `GROSS AMT` → `amount` (DOUBLE)
- Derives: `year`, `month`, `quarter`, `month_name`

**Usage**: This is the **primary fact table** for all revenue, sales, and performance queries.

---

### 2️⃣ `product_dim_view`
**Purpose**: Master product catalog

**Grain**: One row = one unique SKU

**Sources** (with precedence):
1. Inventory (highest priority)
2. Amazon sales
3. International sales

**Attributes**:
```
sku, category, size, style, asin, data_sources, category_clean, product_type
```

**Conflict Resolution**:
- When SKU exists in multiple sources, attributes are resolved by precedence
- Example: If Inventory has `category = 'Kurta'` and Amazon has `category = 'kurta'`, Inventory wins

**Usage**: Join to `sales_fact_view` or `inventory_dim_view` for product enrichment

---

### 3️⃣ `inventory_dim_view`
**Purpose**: Current inventory snapshot

**Grain**: One row = one SKU's inventory status

**Attributes**:
```
sku, category, size, stock, stock_status, is_out_of_stock, category_clean
```

**Derived Fields**:
- `stock_status`:
  - `'Out of Stock'` (stock = 0)
  - `'Low Stock'` (stock < 10)
  - `'Medium Stock'` (stock < 50)
  - `'High Stock'` (stock >= 50)
- `is_out_of_stock`: Binary flag (1 = yes, 0 = no)

**Usage**: Inventory analysis, stock alerts, join to sales for sell-through rates

---

## Governance Artifacts

### 📋 `metric_dictionary.json`
**Purpose**: Single source of truth for all metrics

**Contents**:
- Metric definitions
- Source columns and views
- Allowed aggregations
- Grain restrictions
- Business logic

**Metrics Defined**:
- `sales_amount`
- `units_sold`
- `avg_order_value`
- `order_count`
- `stock_level`
- `out_of_stock_count`

**Why This Matters**: Phase 3 agents will read this file to understand what metrics exist and how to compute them correctly.

---

### 📊 `dataset_catalog.json`
**Purpose**: Metadata repository for all datasets

**Contents**:
- Dataset descriptions
- Row counts
- Available metrics and dimensions
- Primary/foreign keys
- Data quality rules
- Refresh frequency

**Why This Matters**: This is the "map" agents use to navigate the data landscape. Essential for RAG (Retrieval-Augmented Generation).

---

## Setup & Execution

### Prerequisites
```bash
pip install -r requirements.txt
```

**Requirements**:
- Python 3.10+
- DuckDB 0.9+
- Pandas 2.0+

---

### Step 1: Place Raw Data
Ensure all CSV files are in `data/raw/`:
```
data/raw/
├── Amazon sale report.csv
├── International sale report.csv
├── sale_report_inventory.csv
├── pricing_may_2022.csv
└── pricing_pl_march_2021.csv
```

---

### Step 2: Run Ingestion
```bash
cd src/ingestion
python run_all_ingestion.py
```

**What Happens**:
1. Creates `db/retail.duckdb`
2. Loads 5 raw tables
3. Logs row counts and data quality checks
4. Output: `logs/ingestion.log`

**Expected Output**:
```
✓ Loaded 128,976 rows into amazon_sales_raw
✓ Loaded 18,459 rows into international_sales_raw
✓ Loaded 1,235 rows into inventory_raw
✓ Loaded X rows into pricing_may_2022_raw
✓ Loaded X rows into pricing_pl_march_2021_raw
```

---

### Step 3: Create Analytical Views
```bash
cd src/transformations
python create_views.py
```

**What Happens**:
1. Executes SQL view definitions
2. Validates row counts and data integrity
3. Output: `logs/transformations.log`

**Expected Output**:
```
✓ sales_fact_view created (145,823 rows)
✓ product_dim_view created (3,487 SKUs)
✓ inventory_dim_view created (1,235 SKUs)
```

---

### Step 4: Verify (Exit Criteria)
Open a DuckDB connection and run:

```sql
-- Check view exists
SHOW TABLES;

-- Answer a business question using a single query
SELECT 
    category,
    country,
    ROUND(SUM(amount), 2) as total_sales,
    SUM(qty) as total_units
FROM sales_fact_view
WHERE year = 2022
GROUP BY category, country
ORDER BY total_sales DESC
LIMIT 10;
```

**Can you**:
1. ✅ Answer "Total sales by category and country" with one query?
2. ✅ Explain where the data came from? (Amazon + International, excluding cancelled)
3. ✅ Explain why it's trustworthy? (Governed by `metric_dictionary.json`, filtered in views)

**If yes → Phase 2 is complete.**

---

## Governance Assumptions

### Data Freshness
- **Static snapshot**: Data is not real-time
- **Coverage**: ~March 2021 to May 2022
- **Refresh**: On-demand (re-run ingestion scripts)

### Data Completeness
- **International sales lack**:
  - Order IDs
  - Country-level detail
- **Pricing tables**: Not yet integrated (Phase 3 candidate)

### Business Rules (Locked Decisions)
1. **Cancelled orders are excluded** from all analytical views
2. **Inventory is point-in-time**, not historical
3. **Conflict resolution**: Inventory > Amazon > International

---

## What NOT in Phase 2 ❌

- No CrewAI
- No Gemini/LLM calls
- No Streamlit UI
- No FAISS / Vector DB
- No agentic workflows

**Why**: Phase 2 is purely data engineering. If you can answer business questions with SQL, you're ready for Phase 3.

---

## Next: Phase 3 (GenAI Integration)

Phase 3 will:
1. Wrap this data layer with **CrewAI agents**:
   - Language-to-SQL agent
   - Data extraction agent
   - Validation agent
2. Add **Gemini API** for NLQ (Natural Language Querying)
3. Add **FAISS** for semantic search on metadata
4. Add **Streamlit** for conversational UI

**Critical**: Everything built in Phase 2 is **foundation, not throwaway**. Phase 3 builds on top of this solid data layer.

---

## Troubleshooting

### Issue: "File not found"
- **Check**: CSV files are in `data/raw/` with exact names
- **Note**: File names are case-sensitive on Linux/Mac

### Issue: "DuckDB connection error"
- **Check**: `db/` directory exists
- **Try**: Delete `db/retail.duckdb` and re-run ingestion

### Issue: "View creation failed"
- **Check**: Raw tables loaded successfully (Step 2)
- **Check**: `logs/transformations.log` for specific SQL errors

### Issue: "Row counts don't match"
- **Expected**: Analytical views will have fewer rows than raw tables (due to filtering)
- **Verify**: Check logs for "Null dates", "Cancelled orders" counts

---

## Logs Location
All logs written to `logs/`:
- `ingestion.log` - Raw data loading
- `transformations.log` - View creation and validation

---

## Contact / Questions
For Phase 2 issues, check:
1. Logs in `logs/`
2. SQL view definitions in `src/transformations/`
3. Metadata in `src/metadata/`

Phase 2 Owner: Data Engineering Team