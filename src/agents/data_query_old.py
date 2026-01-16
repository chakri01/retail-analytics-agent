"""
Unified Data Query Agent for cross-dataset queries
"""
import pandas as pd
from typing import Dict, Any
from src.utils.postgres_connection import get_connection
from src.vector_db.metadata_catalog import MetadataCatalog
import json

class DataQueryAgent:
    """Agent 2: Convert intent to SQL for cross-view queries"""
    
    def __init__(self):
        self.conn = get_connection()
        self.metadata = MetadataCatalog()
    
    def execute(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Execute query based on intent and return results"""
        try:
            # Build SQL from intent
            sql = self._build_unified_sql(intent)
            
            if not sql:
                return {
                    "success": False,
                    "error": "Could not build SQL for intent",
                    "data": [],
                    "row_count": 0,
                    "columns": [],
                    "sql": ""
                }
            
            # Execute query
            df = pd.read_sql_query(sql, self.conn)
            
            # Log query execution
            self._log_query(intent, sql, len(df))
            
            return {
                "success": True,
                "data": df.to_dict(orient='records'),
                "row_count": len(df),
                "columns": list(df.columns),
                "sql": sql
            }
            
        except Exception as e:
            print(f"Data query error: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "row_count": 0,
                "columns": [],
                "sql": ""
            }
    
    def _build_unified_sql(self, intent: Dict[str, Any]) -> str:
        """Build SQL query that can span multiple views"""
        metrics = intent.get("metrics", [])
        dimensions = intent.get("dimensions", [])
        filters = intent.get("filters", {})
        intent_type = intent.get("intent_type", "aggregate")

        # Determine which views are needed
        needed_views = self._determine_needed_views(metrics, dimensions)

        if not needed_views:
            return ""

        # Build FROM clause with joins
        from_clause = self._build_from_clause(needed_views)

        # Build SELECT clause
        select_parts = []

        # Add dimensions
        for dim in dimensions:
            # Find which view has this dimension
            view = self._find_view_for_column(dim, needed_views)
            if view:
                select_parts.append(f"{view}.{dim} AS {dim}")

        # Add metrics - handle differently based on intent_type
        for metric in metrics:
            if intent_type == "aggregate":
                # For aggregate queries, add SUM/COUNT etc.
                metric_sql = self._get_aggregate_metric_sql(metric, needed_views)
            else:
                # For other queries (like filter), use the column directly
                metric_sql = self._get_column_metric_sql(metric, needed_views)

            if metric_sql:
                select_parts.append(f"{metric_sql} AS {metric}")

        select_clause = ", ".join(select_parts) if select_parts else "*"

        # Build WHERE clause
        where_clause = self._build_where_clause(filters, needed_views)

        # Build GROUP BY clause
        group_by_parts = []
        for dim in dimensions:
            view = self._find_view_for_column(dim, needed_views)
            if view:
                group_by_parts.append(f"{view}.{dim}")

        group_by_clause = ", ".join(group_by_parts) if group_by_parts else ""

        # Build ORDER BY clause
        order_by_clause = ""
        if intent_type == "top" and metrics:
            # Order by first metric descending for top queries
            order_by_clause = f"ORDER BY {metrics[0]} DESC"
        elif metrics and not dimensions:
            # If no dimensions but metrics, still order by first metric
            order_by_clause = f"ORDER BY {metrics[0]} DESC"

        # Assemble SQL
        sql = f"SELECT {select_clause} FROM {from_clause}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        if group_by_clause:
            sql += f" GROUP BY {group_by_clause}"

        if order_by_clause:
            sql += f" {order_by_clause}"

        # Add LIMIT for top N queries
        if intent_type == "top" and "top_n" in filters:
            sql += f" LIMIT {filters['top_n']}"
        elif intent_type == "top":
            sql += " LIMIT 10"

        return sql

    def _get_aggregate_metric_sql(self, metric: str, needed_views: list) -> str:
        """Get SQL expression for a metric in aggregate queries"""
        # List of common aggregate functions
        agg_functions = ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX']

        # Check if metric already contains an aggregate function
        metric_upper = metric.upper()
        if any(agg in metric_upper for agg in agg_functions):
            return metric

        # Metric mappings for aggregate queries
        metric_mappings = {
            "amount": "SUM(sales_fact_view.amount)",
            "sales_amount": "SUM(sales_fact_view.amount)",
            "qty": "SUM(sales_fact_view.qty)",
            "units_sold": "SUM(sales_fact_view.qty)",
            "order_count": "COUNT(DISTINCT sales_fact_view.order_id)",
            "stock": "SUM(inventory_dim_view.stock)",
            "current_stock": "SUM(inventory_dim_view.stock)",
            "low_stock_count": "SUM(CASE WHEN inventory_dim_view.stock_status = 'Low Stock' THEN 1 ELSE 0 END)",
            "avg_order_value": "AVG(sales_fact_view.amount)",
            "total_products": "COUNT(DISTINCT product_dim_view.sku)"
        }

        if metric in metric_mappings:
            return metric_mappings[metric]

        # Default to SUM for numeric columns
        for view_name in needed_views:
            if view_name in self.metadata.views:
                if metric in self.metadata.views[view_name]["columns"]:
                    return f"SUM({view_name}.{metric})"

        return metric  # Fallback

    def _get_column_metric_sql(self, metric: str, needed_views: list) -> str:
        """Get SQL expression for a metric in non-aggregate queries"""
        # For filter/top queries, just use the column directly
        for view_name in needed_views:
            if view_name in self.metadata.views:
                if metric in self.metadata.views[view_name]["columns"]:
                    return f"{view_name}.{metric}"
    
        return metric
    
    def _determine_needed_views(self, metrics: list, dimensions: list) -> list:
        """Determine which views are needed for the query"""
        needed_views = set()
        
        # Check all columns in all views
        all_columns = {}
        for view_name, info in self.metadata.views.items():
            for column in info["columns"].keys():
                all_columns[column] = view_name
        
        # Check metrics and dimensions
        for item in metrics + dimensions:
            if item in all_columns:
                needed_views.add(all_columns[item])
        
        # Also add related views for joins
        result = list(needed_views)
        
        # If we need multiple views, ensure we have the connecting views
        if len(result) > 1:
            # Add sales_fact_view if it can connect others
            if any(v != "sales_fact_view" for v in result):
                result.append("sales_fact_view")
        
        return result
    
    def _build_from_clause(self, views: list) -> str:
        """Build FROM clause with appropriate joins"""
        if len(views) == 1:
            return views[0]
        
        # Start with sales_fact_view as the primary
        if "sales_fact_view" in views:
            primary = "sales_fact_view"
            others = [v for v in views if v != "sales_fact_view"]
        else:
            primary = views[0]
            others = views[1:]
        
        from_clause = primary
        
        for other_view in others:
            from_clause += f" JOIN {other_view} ON {primary}.sku = {other_view}.sku"
        
        return from_clause
    
    def _find_view_for_column(self, column: str, available_views: list) -> str:
        """Find which view contains the column"""
        for view_name, info in self.metadata.views.items():
            if view_name in available_views and column in info["columns"]:
                return view_name
        return ""
    
    def _get_metric_sql(self, metric: str, needed_views: list) -> str:
        """Get SQL expression for a metric"""
        # Simple metric mappings
        metric_mappings = {
            "sales_amount": "SUM(sales_fact_view.amount)",
            "units_sold": "SUM(sales_fact_view.qty)",
            "order_count": "COUNT(DISTINCT sales_fact_view.order_id)",
            "current_stock": "SUM(inventory_dim_view.stock)",
            "low_stock_count": "SUM(CASE WHEN inventory_dim_view.stock_status = 'Low Stock' THEN 1 ELSE 0 END)",
            "avg_order_value": "AVG(sales_fact_view.amount)",
            "total_products": "COUNT(DISTINCT product_dim_view.sku)"
        }
        
        return metric_mappings.get(metric, metric)
    
    def _build_where_clause(self, filters: dict, needed_views: list) -> str:
        """Build WHERE clause for filters"""
        where_parts = []
        
        for field, value in filters.items():
            if field == "top_n":
                continue
            
            # Find which view has this field
            view = self._find_view_for_column(field, needed_views)
            if not view:
                continue
            
            if isinstance(value, list):
                values_str = ", ".join([f"'{v}'" for v in value])
                where_parts.append(f"{view}.{field} IN ({values_str})")
            elif isinstance(value, str):
                where_parts.append(f"{view}.{field} = '{value}'")
            else:
                where_parts.append(f"{view}.{field} = {value}")
        
        return " AND ".join(where_parts)
    
    def _log_query(self, intent: Dict[str, Any], sql: str, row_count: int):
        """Log query execution"""
        log_entry = {
            "intent": intent,
            "sql": sql,
            "row_count": row_count,
            "timestamp": "2024-01-01 00:00:00"
        }
        
        # Append to log file
        with open("logs/data_queries.log", "a") as f:
            f.write(json.dumps(log_entry) + "\n")