-- ============================================================================
-- SALES FACT VIEW
-- Unified sales data from Amazon and International channels
-- Grain: One row = one sold SKU in an order
-- ============================================================================

CREATE OR REPLACE VIEW sales_fact_view AS

-- Amazon Sales (domestic)
SELECT 
    order_id,
    date AS order_date,
    sku,
    category,
    size,
    style,
    qty,
    amount,
    'Amazon' AS channel,
    ship_country AS country,
    ship_state AS state,
    ship_city AS city,
    currency,
    fulfilment,
    sales_channel,
    b2b,
    
    -- Derived time dimensions
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(QUARTER FROM date) AS quarter,
    to_char(date,'Month') AS month_name
    
FROM amazon_sales_raw

WHERE 1=1
    -- CRITICAL: Exclude cancelled orders
    AND LOWER(status) NOT LIKE '%cancelled%'
    -- Data quality filters
    AND date IS NOT NULL
    AND sku IS NOT NULL
    AND sku != ''
    AND amount IS NOT NULL

UNION ALL

-- International Sales
SELECT 
    NULL AS order_id,  -- International data doesn't have order IDs
    date AS order_date,
    sku,
    NULL AS category,  -- Will be enriched from product_dim later
    size,
    style,
    pcs AS qty,
    gross_amt AS amount,
    'International' AS channel,
    NULL AS country,  -- International data doesn't have country breakdown
    NULL AS state,
    NULL AS city,
    NULL AS currency,  -- Assuming standard currency for international
    NULL AS fulfilment,
    customer AS sales_channel,  -- Using customer as sales channel proxy
    NULL AS b2b,
    
    -- Derived time dimensions
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(QUARTER FROM date) AS quarter,
    to_char(date,'Month') AS month_name
    
FROM international_sales_raw

WHERE 1=1
    -- Data quality filters
    AND date IS NOT NULL
    AND sku IS NOT NULL
    AND sku != ''
    AND gross_amt IS NOT NULL
    AND pcs IS NOT NULL;

-- ============================================================================
-- VALIDATION QUERIES (run after view creation)
-- ============================================================================

-- Total rows should be less than sum of raw tables (due to cancelled filter)
-- SELECT COUNT(*) as total_rows FROM sales_fact_view;

-- Check no cancelled orders leaked through
-- SELECT COUNT(*) FROM sales_fact_view WHERE channel = 'Amazon' 
--   AND order_id IN (SELECT order_id FROM amazon_sales_raw WHERE LOWER(status) LIKE '%cancelled%');

-- Date range check
-- SELECT MIN(order_date) as earliest_date, MAX(order_date) as latest_date FROM sales_fact_view;

-- Channel breakdown
-- SELECT channel, COUNT(*) as row_count, SUM(amount) as total_sales FROM sales_fact_view GROUP BY channel;