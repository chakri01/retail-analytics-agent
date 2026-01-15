"""
Dataset metadata catalog for FAISS indexing
"""
from typing import List, Dict, Any
import json

class MetadataCatalog:
    """Catalog of datasets, metrics, and column descriptions"""
    
    def __init__(self):
        self.datasets = {
            "amazon_sales": {
                "description": "Amazon sales transactions with product and regional data",
                "source_view": "sales_fact_view",
                "metrics": [
                    {"name": "sales_amount", "description": "Total sales revenue", "sql": "SUM(amount)"},
                    {"name": "units_sold", "description": "Number of units sold", "sql": "SUM(qty)"},
                    {"name": "order_count", "description": "Number of orders", "sql": "COUNT(DISTINCT order_id)"},
                    {"name": "avg_order_value", "description": "Average order value", "sql": "AVG(amount)"}
                ],
                "dimensions": [
                    {"name": "category", "description": "Product category", "sql": "category"},
                    {"name": "country", "description": "Country code (IN, US, etc.)", "sql": "country"},
                    {"name": "region", "description": "State/region", "sql": "state"},
                    {"name": "month", "description": "Month number", "sql": "month"},
                    {"name": "year", "description": "Year", "sql": "year"},
                    {"name": "product_name", "description": "Product style/name", "sql": "style"},
                    {"name": "channel", "description": "Sales channel", "sql": "channel"},
                    {"name": "fulfilment", "description": "Fulfilment method", "sql": "fulfilment"}
                ],
                "date_column": "order_date",
                "text_columns": ["category", "country", "state", "city", "style"]
            },
            "inventory": {
                "description": "Current inventory levels and stock status",
                "source_view": "inventory_dim_view",
                "metrics": [
                    {"name": "current_stock", "description": "Total units in stock", "sql": "SUM(stock)"},
                    {"name": "low_stock_count", "description": "Number of low stock items", "sql": "SUM(CASE WHEN stock_status = 'Low Stock' THEN 1 ELSE 0 END)"},
                    {"name": "out_of_stock_count", "description": "Number of out of stock items", "sql": "SUM(is_out_of_stock)"},
                    {"name": "total_products", "description": "Total number of products", "sql": "COUNT(*)"}
                ],
                "dimensions": [
                    {"name": "category_clean", "description": "Product category", "sql": "category_clean"},
                    {"name": "size", "description": "Product size", "sql": "size"},
                    {"name": "stock_status", "description": "Stock status", "sql": "stock_status"}
                ],
                "text_columns": ["category_clean", "size", "stock_status"]
            },
            "products": {
                "description": "Product catalog with SKUs and categories",
                "source_view": "product_dim_view",
                "metrics": [
                    {"name": "product_count", "description": "Number of products", "sql": "COUNT(*)"},
                    {"name": "amazon_products", "description": "Products on Amazon", "sql": "SUM(CASE WHEN data_sources LIKE '%Amazon%' THEN 1 ELSE 0 END)"}
                ],
                "dimensions": [
                    {"name": "category_clean", "description": "Product category", "sql": "category_clean"},
                    {"name": "size", "description": "Product size", "sql": "size"},
                    {"name": "product_type", "description": "Type of product", "sql": "product_type"}
                ]
            }
        }
        
        # Create text chunks for FAISS indexing
        self.text_chunks = self._create_text_chunks()
    
    def _create_text_chunks(self) -> List[str]:
        """Convert metadata to text chunks for FAISS"""
        chunks = []
        
        for dataset_name, info in self.datasets.items():
            # Dataset description chunk
            chunks.append(f"Dataset: {dataset_name}. {info['description']}. Source view: {info['source_view']}")
            
            # Metrics chunk
            metrics_text = ", ".join([m['name'] for m in info['metrics']])
            chunks.append(f"{dataset_name} metrics include: {metrics_text}")
            
            # Dimensions chunk
            dims_text = ", ".join([d['name'] for d in info['dimensions']])
            chunks.append(f"{dataset_name} dimensions include: {dims_text}")
            
            # Individual metric descriptions
            for metric in info['metrics']:
                chunks.append(f"Metric '{metric['name']}' in {dataset_name}: {metric['description']}. SQL: {metric.get('sql', 'N/A')}")
            
            # Individual dimension descriptions
            for dim in info['dimensions']:
                chunks.append(f"Dimension '{dim['name']}' in {dataset_name}: {dim['description']}")
        
        # Add common queries
        chunks.append("Common queries: total sales by category, top products, inventory status, sales by country")
        chunks.append("Filter examples: by country (IN, US), by category (Electronics, Clothing), by date range")
        
        return chunks
    
    def get_dataset_info(self, dataset_name: str) -> Dict[str, Any]:
        """Get metadata for a specific dataset"""
        return self.datasets.get(dataset_name, {})
    
    def get_all_datasets(self) -> List[str]:
        """Get list of all dataset names"""
        return list(self.datasets.keys())
    
    def get_metric_sql(self, dataset_name: str, metric_name: str) -> str:
        """Get SQL expression for a metric"""
        dataset = self.get_dataset_info(dataset_name)
        for metric in dataset.get('metrics', []):
            if metric['name'] == metric_name:
                return metric.get('sql', metric_name)
        return metric_name
    
    def get_dimension_sql(self, dataset_name: str, dimension_name: str) -> str:
        """Get SQL expression for a dimension"""
        dataset = self.get_dataset_info(dataset_name)
        for dimension in dataset.get('dimensions', []):
            if dimension['name'] == dimension_name:
                return dimension.get('sql', dimension_name)
        return dimension_name
    
    def is_valid_metric(self, dataset_name: str, metric_name: str) -> bool:
        """Check if metric exists in dataset"""
        dataset = self.get_dataset_info(dataset_name)
        return any(m['name'] == metric_name for m in dataset.get('metrics', []))
    
    def is_valid_dimension(self, dataset_name: str, dimension_name: str) -> bool:
        """Check if dimension exists in dataset"""
        dataset = self.get_dataset_info(dataset_name)
        return any(d['name'] == dimension_name for d in dataset.get('dimensions', []))