-- ============================================================================
-- INVENTORY DIMENSION VIEW
-- Clean inventory snapshot with stock levels
-- Grain: One row = one SKU's inventory status
-- ============================================================================

CREATE OR REPLACE VIEW inventory_dim_view AS

SELECT 
    sku_code AS sku,
    category,
    size,
    stock,
    
    -- Derived metrics
    CASE 
        WHEN stock = 0 THEN 'Out of Stock'
        WHEN stock < 10 THEN 'Low Stock'
        WHEN stock < 50 THEN 'Medium Stock'
        ELSE 'High Stock'
    END AS stock_status,
    
    CASE 
        WHEN stock = 0 THEN 1
        ELSE 0
    END AS is_out_of_stock,
    
    -- Clean category
    UPPER(TRIM(category)) AS category_clean

FROM inventory_raw

WHERE 1=1
    -- Data quality filters
    AND sku_code IS NOT NULL
    AND sku_code != '';

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- Total SKUs in inventory
-- SELECT COUNT(*) as total_skus FROM inventory_dim_view;

-- Stock status distribution
-- SELECT stock_status, COUNT(*) as sku_count, SUM(stock) as total_units 
-- FROM inventory_dim_view GROUP BY stock_status ORDER BY sku_count DESC;

-- Out of stock items
-- SELECT COUNT(*) as out_of_stock_count FROM inventory_dim_view WHERE is_out_of_stock = 1;

-- Top categories by inventory
-- SELECT category, COUNT(*) as sku_count, SUM(stock) as total_stock 
-- FROM inventory_dim_view GROUP BY category ORDER BY total_stock DESC LIMIT 10;