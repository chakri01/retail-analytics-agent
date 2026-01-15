"""
Agent 2: Data Query Agent (NO LLM creativity)
Responsibility: Convert intent → SQL → Execute → Return results
"""
import pandas as pd
from typing import Dict, Any, List
from src.utils.postgres_connection import get_connection
from src.vector_db.metadata_catalog import MetadataCatalog
import json

class DataQueryAgent:
    """Agent 2: Convert intent to SQL and execute"""
    
    def __init__(self):
        self.conn = get_connection()
        self.metadata_catalog = MetadataCatalog()
    
    def execute(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Execute query based on intent and return results"""
        try:
            # Build SQL from intent
            sql = self._build_sql(intent)
            
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
    
    def _build_sql(self, intent: Dict[str, Any]) -> str:
        """Build SQL query from intent using templates"""
        dataset = intent.get("dataset", "")
        intent_type = intent.get("intent_type", "")
        metrics = intent.get("metrics", [])
        dimensions = intent.get("dimensions", [])
        filters = intent.get("filters", {})
        
        # Get dataset info
        dataset_info = self.metadata_catalog.get_dataset_info(dataset)
        if not dataset_info:
            return ""
        
        source_view = dataset_info.get("source_view", "")
        
        # Build SELECT clause
        select_parts = []
        
        # Add dimensions
        for dim in dimensions:
            sql_dim = self.metadata_catalog.get_dimension_sql(dataset, dim)
            select_parts.append(f"{sql_dim} AS {dim}")
        
        # Add metrics
        for metric in metrics:
            sql_metric = self.metadata_catalog.get_metric_sql(dataset, metric)
            select_parts.append(f"{sql_metric} AS {metric}")
        
        if not select_parts:
            select_parts.append("*")
        
        select_clause = ", ".join(select_parts)
        
        # Build WHERE clause
        where_parts = []
        
        for field, value in filters.items():
            if field == "top_n":
                continue  # Handled in LIMIT
            
            # Get correct SQL field name
            sql_field = self.metadata_catalog.get_dimension_sql(dataset, field)
            if not sql_field:
                sql_field = field
            
            if isinstance(value, list):
                values_str = ", ".join([f"'{v}'" for v in value])
                where_parts.append(f"{sql_field} IN ({values_str})")
            elif isinstance(value, str):
                where_parts.append(f"{sql_field} = '{value}'")
            else:
                where_parts.append(f"{sql_field} = {value}")
        
        where_clause = " AND ".join(where_parts)
        
        # Build GROUP BY clause
        group_by_parts = []
        for dim in dimensions:
            sql_dim = self.metadata_catalog.get_dimension_sql(dataset, dim)
            group_by_parts.append(sql_dim)
        
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
        sql = f"SELECT {select_clause} FROM {source_view}"
        
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
            sql += " LIMIT 10"  # Default limit for top queries
        
        return sql
    
    def _log_query(self, intent: Dict[str, Any], sql: str, row_count: int):
        """Log query execution"""
        log_entry = {
            "intent": intent,
            "sql": sql,
            "row_count": row_count,
            "timestamp": "2024-01-01 00:00:00"  # TODO: Add actual timestamp
        }
        
        # Append to log file
        with open("logs/data_queries.log", "a") as f:
            f.write(json.dumps(log_entry) + "\n")