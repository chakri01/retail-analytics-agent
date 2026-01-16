"""
Unified metadata catalog for cross-dataset queries
"""
from typing import List, Dict, Any
import json

class MetadataCatalog:
    """Catalog of all datasets with relationships"""
    
    def __init__(self):
        self.views = {
            "sales_fact_view": {
                "description": "Unified sales transactions across all channels and regions",
                "columns": {
                    "order_id": "Unique order identifier",
                    "order_date": "Date of the order",
                    "sku": "Stock Keeping Unit (joins to product_dim_view)",
                    "category": "Product category",
                    "size": "Product size",
                    "style": "Product style/name",
                    "qty": "Quantity sold",
                    "amount": "Sales amount",
                    "channel": "Sales channel (Amazon, International)",
                    "country": "Country code",
                    "state": "State/region",
                    "city": "City",
                    "currency": "Currency",
                    "fulfilment": "Fulfilment method",
                    "year": "Year derived from order_date",
                    "month": "Month derived from order_date",
                    "quarter": "Quarter derived from order_date",
                    "month_name": "Month name"
                },
                "relationships": [
                    {"view": "product_dim_view", "on": "sku"},
                    {"view": "inventory_dim_view", "on": "sku"}
                ],
                "primary_key": "order_id",
                "foreign_keys": ["sku"]
            },
            "product_dim_view": {
                "description": "Master product catalog with all SKUs",
                "columns": {
                    "sku": "Stock Keeping Unit (joins to sales_fact_view and inventory_dim_view)",
                    "category": "Product category",
                    "size": "Product size",
                    "style": "Product style/name",
                    "asin": "Amazon Standard Identification Number",
                    "data_sources": "Sources of product data (Inventory, Amazon, International)",
                    "category_clean": "Cleaned category name",
                    "product_type": "Type of product"
                },
                "relationships": [
                    {"view": "sales_fact_view", "on": "sku"},
                    {"view": "inventory_dim_view", "on": "sku"}
                ],
                "primary_key": "sku"
            },
            "inventory_dim_view": {
                "description": "Current inventory levels and stock status",
                "columns": {
                    "sku": "Stock Keeping Unit (joins to sales_fact_view and product_dim_view)",
                    "category": "Product category",
                    "size": "Product size",
                    "stock": "Current stock quantity",
                    "stock_status": "Stock status (Out of Stock, Low Stock, etc.)",
                    "is_out_of_stock": "Binary flag for out of stock items",
                    "category_clean": "Cleaned category name"
                },
                "relationships": [
                    {"view": "sales_fact_view", "on": "sku"},
                    {"view": "product_dim_view", "on": "sku"}
                ],
                "primary_key": "sku"
            }
        }
        
        # Create unified metrics (can span multiple views)
        self.unified_metrics = {
            "sales_performance": {
                "description": "Sales metrics across all channels",
                "calculation": "sales_fact_view.amount",
                "aggregations": ["SUM", "AVG", "COUNT", "MIN", "MAX"],
                "related_views": ["sales_fact_view"]
            },
            "inventory_turnover": {
                "description": "Inventory turnover ratio",
                "calculation": "sales_fact_view.qty / inventory_dim_view.stock",
                "aggregations": ["AVG", "SUM"],
                "related_views": ["sales_fact_view", "inventory_dim_view"]
            },
            "product_performance": {
                "description": "Product-level performance metrics",
                "calculation": "Multiple metrics",
                "aggregations": ["SUM", "COUNT"],
                "related_views": ["sales_fact_view", "product_dim_view"]
            }
        }
        
        # Common business questions
        self.common_queries = [
            "Which category has the highest sales?",
            "What are the top 10 products by sales?",
            "Which products are low in stock but high in sales?",
            "Compare sales performance across countries",
            "What is the monthly sales trend?",
            "Which products are out of stock?",
            "What is the average order value by category?",
            "Which region has the highest sales growth?"
        ]
        
        # Create text chunks for FAISS
        self.text_chunks = self._create_text_chunks()
    
    def _create_text_chunks(self) -> List[str]:
        """Convert metadata to text chunks for FAISS"""
        chunks = []
        
        # View descriptions
        for view_name, info in self.views.items():
            chunks.append(f"View: {view_name}. {info['description']}")
            chunks.append(f"{view_name} has columns: {', '.join(info['columns'].keys())}")
            
            # Add column descriptions
            for col_name, col_desc in info['columns'].items():
                chunks.append(f"Column {col_name} in {view_name}: {col_desc}")
        
        # Relationships
        chunks.append("Views are related by: sales_fact_view.sku = product_dim_view.sku = inventory_dim_view.sku")
        chunks.append("You can join sales data with product info using the SKU column")
        chunks.append("You can join sales data with inventory using the SKU column")
        
        # Metrics
        for metric_name, info in self.unified_metrics.items():
            chunks.append(f"Metric {metric_name}: {info['description']}. Uses views: {', '.join(info['related_views'])}")
        
        # Common queries
        chunks.append("Common business questions: " + "; ".join(self.common_queries))
        
        return chunks
    
    def get_view_info(self, view_name: str) -> Dict[str, Any]:
        """Get metadata for a specific view"""
        return self.views.get(view_name, {})
    
    def get_all_views(self) -> List[str]:
        """Get list of all view names"""
        return list(self.views.keys())
    
    def get_related_views(self, view_name: str) -> List[str]:
        """Get views related to this view"""
        info = self.get_view_info(view_name)
        return [rel["view"] for rel in info.get("relationships", [])]