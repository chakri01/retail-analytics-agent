-- ============================================================================
-- PRODUCT DIMENSION VIEW
-- One row per unique SKU with product attributes
-- Grain: One row = one SKU
-- ============================================================================

CREATE OR REPLACE VIEW product_dim_view AS

WITH amazon_products AS (
    -- Extract distinct products from Amazon sales
    SELECT DISTINCT
        sku,
        category,
        size,
        style,
        asin,
        'Amazon' AS source
    FROM amazon_sales_raw
    WHERE sku IS NOT NULL 
      AND sku != ''
),

international_products AS (
    -- Extract distinct products from International sales
    SELECT DISTINCT
        sku,
        NULL AS category,  -- International data lacks category
        size,
        style,
        NULL AS asin,
        'International' AS source
    FROM international_sales_raw
    WHERE sku IS NOT NULL 
      AND sku != ''
),

inventory_products AS (
    -- Extract distinct products from Inventory
    -- Note: Column names will be determined at runtime based on actual inventory structure
    SELECT DISTINCT
        sku_code AS sku,
        category,
        size,
        NULL AS style,
        NULL AS asin,
        'Inventory' AS source
    FROM inventory_raw
    WHERE sku_code IS NOT NULL 
      AND sku_code != ''
),

all_products AS (
    -- Combine all product sources
    SELECT * FROM amazon_products
    UNION ALL
    SELECT * FROM international_products
    UNION ALL
    SELECT * FROM inventory_products
),

-- Resolve conflicts using precedence: Inventory > Amazon > International
product_master AS (
    SELECT 
        sku,
        -- Take first non-null value with precedence
        COALESCE(
            MAX(CASE WHEN source = 'Inventory' THEN category END),
            MAX(CASE WHEN source = 'Amazon' THEN category END),
            MAX(CASE WHEN source = 'International' THEN category END)
        ) AS category,
        
        COALESCE(
            MAX(CASE WHEN source = 'Inventory' THEN size END),
            MAX(CASE WHEN source = 'Amazon' THEN size END),
            MAX(CASE WHEN source = 'International' THEN size END)
        ) AS size,
        
        COALESCE(
            MAX(CASE WHEN source = 'Inventory' THEN style END),
            MAX(CASE WHEN source = 'Amazon' THEN style END),
            MAX(CASE WHEN source = 'International' THEN style END)
        ) AS style,
        
        MAX(CASE WHEN source = 'Amazon' THEN asin END) AS asin,
        
        -- Track which sources contributed
        STRING_AGG(DISTINCT source, ', ' ORDER BY source) AS data_sources
        
    FROM all_products
    GROUP BY sku
)

SELECT 
    sku,
    category,
    size,
    style,
    asin,
    data_sources,
    -- Derived attributes
    UPPER(TRIM(category)) AS category_clean,
    CASE 
        WHEN UPPER(size) IN ('XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL', '2XL', '3XL', '4XL', '5XL') 
        THEN 'Clothing'
        ELSE 'Other'
    END AS product_type
FROM product_master;

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- Total unique SKUs
-- SELECT COUNT(*) as unique_skus FROM product_dim_view;

-- SKUs by data source
-- SELECT data_sources, COUNT(*) as sku_count FROM product_dim_view GROUP BY data_sources;

-- Category breakdown
-- SELECT category, COUNT(*) as sku_count FROM product_dim_view GROUP BY category ORDER BY sku_count DESC;