"""Validation Agent for intent validation and result validation"""
from typing import Dict, Any
from src.vector_db.metadata_catalog import MetadataCatalog

class ValidationAgent:
    """Agent 4: Validate intent and results against metadata catalog"""
    
    def __init__(self):
        self.metadata_catalog = MetadataCatalog()
        self.available_views = self.metadata_catalog.get_all_views()
        # Column aliases for validation
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
            "inventory_status": "stock_status",
            "product": "sku",
            "item": "sku"
        }
    
    def validate_intent(self, intent: Dict) -> Dict:
        """Validate intent against metadata catalog"""
        
        # Check required fields
        required_fields = ['dataset', 'intent_type', 'metrics', 'needed_views']
        missing_fields = [field for field in required_fields if field not in intent]
        
        if missing_fields:
            return {
                "valid": False,
                "decision": "block",
                "reason": f"Missing required fields: {missing_fields}",
                "confidence": 0.0
            }
        
        dataset = intent['dataset']
        needed_views = intent['needed_views']
        metrics = intent['metrics']
        
        # 1. Check dataset exists
        if dataset not in self.available_views:
            return {
                "valid": False,
                "decision": "block",
                "reason": f"Dataset '{dataset}' not found. Available: {self.available_views}",
                "confidence": 0.0
            }
        
        # 2. Check all needed views exist
        invalid_views = [view for view in needed_views if view not in self.available_views]
        if invalid_views:
            return {
                "valid": False,
                "decision": "block",
                "reason": f"Views not found: {invalid_views}. Available: {self.available_views}",
                "confidence": 0.0
            }
        
        # 3. Check dataset is in needed_views
        if dataset not in needed_views:
            return {
                "valid": False,
                "decision": "warn",
                "reason": f"Dataset '{dataset}' should be in needed_views",
                "confidence": 0.5,
                "valid": True  # Still allow it
            }
        
        # 4. Check metrics exist in dataset or needed_views (with aliases)
        invalid_metrics = []
        valid_metrics = []
        
        for metric in metrics:
            # Resolve alias
            resolved_metric = self.column_aliases.get(metric, metric)
            
            # Check in dataset first
            dataset_info = self.metadata_catalog.get_view_info(dataset)
            dataset_columns = list(dataset_info.get('columns', {}).keys())
            
            metric_found = False
            
            # Check original metric
            if metric in dataset_columns:
                valid_metrics.append(metric)
                metric_found = True
            # Check resolved metric
            elif resolved_metric in dataset_columns:
                valid_metrics.append(metric)  # Keep original name
                metric_found = True
            else:
                # Check in other needed views
                for view in needed_views:
                    if view == dataset:
                        continue  # Already checked
                    
                    view_info = self.metadata_catalog.get_view_info(view)
                    view_columns = list(view_info.get('columns', {}).keys())
                    
                    if metric in view_columns or resolved_metric in view_columns:
                        valid_metrics.append(metric)
                        metric_found = True
                        break
            
            if not metric_found:
                invalid_metrics.append(metric)
        
        if invalid_metrics:
            # Show available columns from all needed views
            all_columns = []
            for view in needed_views:
                view_info = self.metadata_catalog.get_view_info(view)
                if view_info:
                    all_columns.extend(view_info.get('columns', {}).keys())
            
            # Add aliases to available columns
            for alias, actual in self.column_aliases.items():
                if actual in all_columns and alias not in all_columns:
                    all_columns.append(alias)
            
            unique_columns = list(set(all_columns))
            
            return {
                "valid": False,
                "decision": "block",
                "reason": f"Metrics not found: {invalid_metrics}. Available columns (with aliases): {sorted(unique_columns)[:20]}...",
                "confidence": 0.0
            }
        
        # 5. Validate intent_type
        valid_intent_types = ['aggregate', 'top', 'filter', 'compare', 'trend', 'join', 'clarify']
        if intent['intent_type'] not in valid_intent_types:
            return {
                "valid": False,
                "decision": "block",
                "reason": f"Invalid intent_type: {intent['intent_type']}. Valid: {valid_intent_types}",
                "confidence": 0.0
            }
        
        # All checks passed
        return {
            "valid": True,
            "decision": "approve",
            "reason": "Intent validated successfully",
            "confidence": 0.9,
            "valid_metrics": valid_metrics  # Optional: include validated metrics
        }
    
    def should_proceed(self, validation_result: Dict) -> bool:
        """Determine if processing should proceed based on validation result"""
        if not validation_result:
            return False
        
        # Valid and approved
        if validation_result.get('valid') and validation_result.get('decision') == 'approve':
            return True
        
        # Warning but still valid
        if validation_result.get('valid') and validation_result.get('decision') == 'warn':
            return True
        
        # Not valid
        return False
    
    def validate_results(self, query_result: Dict) -> Dict:
        """
        Validate query results for quality and safety.
        """
        if not query_result:
            return {
                "valid": False,
                "decision": "warn",
                "reason": "No results returned",
                "confidence": 0.0
            }
        
        # Check if query was successful
        if not query_result.get('success', False):
            return {
                "valid": False,
                "decision": "block",
                "reason": f"Query failed: {query_result.get('error', 'Unknown error')}",
                "confidence": 0.0
            }
        
        row_count = query_result.get('row_count', 0)
        
        # Simple validation - just check we have some results
        if row_count == 0:
            return {
                "valid": True,  # Empty can be valid
                "decision": "warn",
                "reason": "Query returned no results",
                "confidence": 0.7
            }
        
        # Success
        return {
            "valid": True,
            "decision": "approve",
            "reason": f"Results validated ({row_count} rows)",
            "confidence": 0.9
        }