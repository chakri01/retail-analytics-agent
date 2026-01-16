"""
Unified Data Query Agent for cross-dataset queries
"""
import pandas as pd
from typing import Dict, Any, List
from src.utils.postgres_connection import get_connection
from src.vector_db.metadata_catalog import MetadataCatalog
import json

class DataQueryAgent:
    """Agent 2: Convert intent to SQL for cross-view queries"""
    
    def __init__(self):
        self.conn = get_connection()
        self.metadata = MetadataCatalog()
        # Create column mappings for common aliases
        self.column_aliases = {
            "sales": "amount",
            "revenue": "amount", 
            "total_sales": "amount",
            "sales_amount": "amount",
            "quantity": "qty",
            "units": "qty",
            "units_sold": "qty",
            "stock_level": "stock",
            "inventory": "stock",
            "current_stock": "stock",
            "status": "stock_status",
            "inventory_status": "stock_status"
        }
    
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
        intent_type = intent.get("intent_type", "aggregate")

        # Route to appropriate SQL builder based on intent type
        if intent_type == "aggregate":
            return self._build_aggregate_sql(intent)
        elif intent_type == "top":
            return self._build_top_sql(intent)
        elif intent_type == "filter":
            return self._build_filter_sql(intent)
        else:
            return self._build_general_sql(intent)

    def _build_aggregate_sql(self, intent: Dict[str, Any]) -> str:
        """Build SQL for aggregate queries"""
        metrics = intent.get("metrics", [])
        dimensions = intent.get("dimensions", [])
        filters = intent.get("filters", {})
        needed_views = intent.get("needed_views", [])

        if not needed_views:
            needed_views = self._determine_needed_views(metrics, dimensions)

        # Build FROM clause
        from_clause = self._build_from_clause(needed_views)

        # Build SELECT clause
        select_parts = []

        # Add dimensions
        for dim in dimensions:
            view = self._find_view_for_column(dim, needed_views)
            if view:
                select_parts.append(f"{view}.{dim} AS {dim}")

        # Add aggregated metrics
        for metric in metrics:
            metric_sql = self._get_aggregate_metric_sql(metric, needed_views)
            if metric_sql:
                select_parts.append(f"{metric_sql} AS {metric}")

        select_clause = ", ".join(select_parts) if select_parts else "*"

        # Build WHERE clause
        where_clause = self._build_where_clause(filters, needed_views)

        # Build GROUP BY
        group_by_parts = []
        for dim in dimensions:
            view = self._find_view_for_column(dim, needed_views)
            if view:
                group_by_parts.append(f"{view}.{dim}")

        group_by_clause = ""
        if group_by_parts:
            group_by_clause = f"GROUP BY {', '.join(group_by_parts)}"

        # Assemble SQL
        sql = f"SELECT {select_clause} FROM {from_clause}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        if group_by_clause:
            sql += f" {group_by_clause}"

        return sql

    def _build_top_sql(self, intent: Dict[str, Any]) -> str:
        """Build SQL for top N queries"""
        metrics = intent.get("metrics", [])
        dimensions = intent.get("dimensions", [])
        filters = intent.get("filters", {})
        needed_views = intent.get("needed_views", [])

        if not needed_views:
            needed_views = self._determine_needed_views(metrics, dimensions)

        # Build FROM clause
        from_clause = self._build_from_clause(needed_views)

        # Build SELECT clause
        select_parts = []

        # Add dimensions
        for dim in dimensions:
            view = self._find_view_for_column(dim, needed_views)
            if view:
                select_parts.append(f"{view}.{dim} AS {dim}")

        # Add aggregated metrics for top queries
        for metric in metrics:
            metric_sql = self._get_aggregate_metric_sql(metric, needed_views)
            if metric_sql:
                select_parts.append(f"{metric_sql} AS {metric}")

        select_clause = ", ".join(select_parts) if select_parts else "*"

        # Build WHERE clause
        where_clause = self._build_where_clause(filters, needed_views)

        # Build GROUP BY
        group_by_parts = []
        for dim in dimensions:
            view = self._find_view_for_column(dim, needed_views)
            if view:
                group_by_parts.append(f"{view}.{dim}")

        group_by_clause = ""
        if group_by_parts:
            group_by_clause = f"GROUP BY {', '.join(group_by_parts)}"

        # Order by first metric (descending for top queries)
        order_by_clause = ""
        if metrics:
            order_by_clause = f"ORDER BY {metrics[0]} DESC"

        # Limit
        limit = filters.get('top_n', 10)

        # Assemble SQL
        sql = f"SELECT {select_clause} FROM {from_clause}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        if group_by_clause:
            sql += f" {group_by_clause}"

        if order_by_clause:
            sql += f" {order_by_clause}"

        sql += f" LIMIT {limit}"

        return sql

    def _build_filter_sql(self, intent: Dict[str, Any]) -> str:
        """Build SQL for filter queries"""
        metrics = intent.get("metrics", [])
        dimensions = intent.get("dimensions", [])
        filters = intent.get("filters", {})
        needed_views = intent.get("needed_views", [])

        if not needed_views:
            needed_views = self._determine_needed_views(metrics + list(filters.keys()), dimensions)

        # Build FROM clause
        from_clause = self._build_from_clause(needed_views)

        # Build SELECT clause - no aggregation for filter queries
        select_parts = []

        # Add dimensions
        for dim in dimensions:
            view = self._find_view_for_column(dim, needed_views)
            if view:
                select_parts.append(f"{view}.{dim} AS {dim}")

        # Add metrics (no aggregation)
        for metric in metrics:
            view = self._find_view_for_column(metric, needed_views)
            if view:
                select_parts.append(f"{view}.{metric} AS {metric}")

        select_clause = ", ".join(select_parts) if select_parts else "*"

        # Build WHERE clause
        where_clause = self._build_where_clause(filters, needed_views)

        # Assemble SQL
        sql = f"SELECT {select_clause} FROM {from_clause}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        # Add limit for safety
        sql += " LIMIT 1000"

        return sql

    def _build_general_sql(self, intent: Dict[str, Any]) -> str:
        """Build SQL for other intent types"""
        metrics = intent.get("metrics", [])
        dimensions = intent.get("dimensions", [])
        filters = intent.get("filters", {})
        needed_views = intent.get("needed_views", [])

        if not needed_views:
            needed_views = self._determine_needed_views(metrics, dimensions)

        # Build FROM clause
        from_clause = self._build_from_clause(needed_views)

        # Build SELECT clause
        select_parts = []

        # Add dimensions
        for dim in dimensions:
            view = self._find_view_for_column(dim, needed_views)
            if view:
                select_parts.append(f"{view}.{dim} AS {dim}")

        # Add metrics
        for metric in metrics:
            metric_sql = self._get_column_metric_sql(metric, needed_views)
            if metric_sql:
                select_parts.append(f"{metric_sql} AS {metric}")

        select_clause = ", ".join(select_parts) if select_parts else "*"

        # Build WHERE clause
        where_clause = self._build_where_clause(filters, needed_views)

        # Assemble SQL
        sql = f"SELECT {select_clause} FROM {from_clause}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        return sql
        
    def _get_aggregate_metric_sql(self, metric: str, needed_views: list) -> str:
        """Get SQL expression for a metric in aggregate queries"""
        # First, resolve any aliases
        resolved_metric = self.column_aliases.get(metric, metric)
        
        # List of common aggregate functions
        agg_functions = ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX']

        # Check if metric already contains an aggregate function
        metric_upper = metric.upper()
        if any(agg in metric_upper for agg in agg_functions):
            return metric

        # Metric mappings for aggregate queries
        metric_mappings = {
            "amount": "SUM(sales_fact_view.amount)",
            "sales": "SUM(sales_fact_view.amount)",  # Added
            "revenue": "SUM(sales_fact_view.amount)",  # Added
            "sales_amount": "SUM(sales_fact_view.amount)",
            "qty": "SUM(sales_fact_view.qty)",
            "units_sold": "SUM(sales_fact_view.qty)",
            "order_count": "COUNT(DISTINCT sales_fact_view.order_id)",
            "stock": "SUM(inventory_dim_view.stock)",
            "current_stock": "SUM(inventory_dim_view.stock)",
            "stock_status": "MAX(inventory_dim_view.stock_status)",  # Changed from SUM to MAX
            "low_stock_count": "SUM(CASE WHEN inventory_dim_view.stock_status = 'Low Stock' THEN 1 ELSE 0 END)",
            "avg_order_value": "AVG(sales_fact_view.amount)",
            "total_products": "COUNT(DISTINCT product_dim_view.sku)"
        }

        if metric in metric_mappings:
            return metric_mappings[metric]
        
        # Also check resolved metric
        if resolved_metric in metric_mappings:
            return metric_mappings[resolved_metric]

        # Default to SUM for numeric columns
        for view_name in needed_views:
            if view_name in self.metadata.views:
                columns = self.metadata.views[view_name]["columns"]
                # Check both original and resolved metric
                for m in [metric, resolved_metric]:
                    if m in columns:
                        # Determine appropriate aggregation
                        column_name = m
                        if column_name in ["stock_status", "category", "size", "style", "channel"]:
                            # Categorical columns use COUNT or MAX
                            return f"MAX({view_name}.{column_name})"
                        else:
                            # Numeric columns use SUM
                            return f"SUM({view_name}.{column_name})"

        return metric  # Fallback

    def _get_column_metric_sql(self, metric: str, needed_views: list) -> str:
        """Get SQL expression for a metric in non-aggregate queries"""
        # First, resolve any aliases
        resolved_metric = self.column_aliases.get(metric, metric)
        
        # For filter queries, just use the column directly
        for view_name in needed_views:
            if view_name in self.metadata.views:
                columns = self.metadata.views[view_name]["columns"]
                # Check both original and resolved metric
                for m in [metric, resolved_metric]:
                    if m in columns:
                        return f"{view_name}.{m}"
    
        return metric
    
    def _determine_needed_views(self, metrics: list, dimensions: list) -> list:
        """Determine which views are needed for the query"""
        needed_views = set()
        
        # Resolve aliases
        resolved_metrics = [self.column_aliases.get(m, m) for m in metrics]
        resolved_dimensions = [self.column_aliases.get(d, d) for d in dimensions]
        
        # Check all columns in all views
        all_columns = {}
        for view_name, info in self.metadata.views.items():
            for column in info["columns"].keys():
                all_columns[column] = view_name
        
        # Check metrics and dimensions (original and resolved)
        for item in metrics + dimensions + resolved_metrics + resolved_dimensions:
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
        # Resolve alias first
        resolved_column = self.column_aliases.get(column, column)
        
        for view_name, info in self.metadata.views.items():
            if view_name in available_views:
                # Check both original and resolved column
                if column in info["columns"] or resolved_column in info["columns"]:
                    return view_name
        return ""
    
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