"""
Agent 3: Validation & Governance Agent (critical)
Responsibility: Check metrics, dimensions, results validity
"""
from typing import Dict, Any
from src.vector_db.metadata_catalog import MetadataCatalog
import json

class ValidationAgent:
    """Agent 3: Validate intent and results against governance rules"""
    
    def __init__(self, confidence_threshold=0.8):
        self.metadata_catalog = MetadataCatalog()
        self.confidence_threshold = confidence_threshold
    
    def validate_intent(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Validate intent before query execution"""
        dataset = intent.get("dataset", "")
        intent_type = intent.get("intent_type", "")
        metrics = intent.get("metrics", [])
        dimensions = intent.get("dimensions", [])
        
        # Check dataset exists
        if not dataset or dataset not in self.metadata_catalog.get_all_datasets():
            return {
                "valid": False,
                "decision": "block",
                "reason": f"Dataset '{dataset}' not found in catalog",
                "confidence": 0.0
            }
        
        # Check intent type
        valid_intent_types = ["aggregate", "compare", "trend", "top", "filter", "clarify"]
        if intent_type not in valid_intent_types:
            return {
                "valid": False,
                "decision": "clarify",
                "reason": f"Intent type '{intent_type}' not supported. Must be one of: {', '.join(valid_intent_types)}",
                "confidence": 0.5
            }
        
        if intent_type == "clarify":
            return {
                "valid": False,
                "decision": "clarify",
                "reason": "Intent unclear, need clarification",
                "confidence": 0.3
            }
        
        # Check metrics
        for metric in metrics:
            if not self.metadata_catalog.is_valid_metric(dataset, metric):
                return {
                    "valid": False,
                    "decision": "clarify",
                    "reason": f"Metric '{metric}' not available in dataset '{dataset}'",
                    "confidence": 0.6
                }
        
        # Check dimensions
        for dimension in dimensions:
            if not self.metadata_catalog.is_valid_dimension(dataset, dimension):
                return {
                    "valid": False,
                    "decision": "clarify",
                    "reason": f"Dimension '{dimension}' not available in dataset '{dataset}'",
                    "confidence": 0.6
                }
        
        # Check if aggregation makes sense
        if intent_type == "aggregate" and not metrics:
            return {
                "valid": False,
                "decision": "clarify",
                "reason": "Aggregate intent requires at least one metric",
                "confidence": 0.7
            }
        
        # All checks passed
        return {
            "valid": True,
            "decision": "proceed",
            "reason": "Intent validated successfully",
            "confidence": 0.9
        }
    
    def validate_results(self, query_result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate query results after execution"""
        if not query_result.get("success", False):
            return {
                "valid": False,
                "decision": "block",
                "reason": f"Query execution failed: {query_result.get('error', 'Unknown error')}",
                "confidence": 0.0
            }
        
        row_count = query_result.get("row_count", 0)
        
        # Check for empty results
        if row_count == 0:
            return {
                "valid": False,
                "decision": "clarify",
                "reason": "Query returned empty results. Consider broadening filters.",
                "confidence": 0.3
            }
        
        # Check for suspiciously large results
        if row_count > 10000:
            return {
                "valid": True,  # Still valid but needs warning
                "decision": "proceed_with_warning",
                "reason": f"Large result set ({row_count} rows). Consider adding filters.",
                "confidence": 0.8
            }
        
        # Check data quality (simple checks)
        data = query_result.get("data", [])
        if data:
            # Check for null values in key metrics
            first_row = data[0]
            for key, value in first_row.items():
                if value is None and any(word in key.lower() for word in ['total', 'sum', 'avg', 'count']):
                    return {
                        "valid": True,
                        "decision": "proceed_with_warning",
                        "reason": f"Null values found in metric '{key}'",
                        "confidence": 0.7
                    }
        
        # All checks passed
        return {
            "valid": True,
            "decision": "proceed",
            "reason": "Results validated successfully",
            "confidence": 0.95
        }
    
    def should_proceed(self, validation_result: Dict[str, Any]) -> bool:
        """Determine if we should proceed based on validation"""
        confidence = validation_result.get("confidence", 0.0)
        decision = validation_result.get("decision", "")
        
        if decision == "block":
            return False
        elif decision == "clarify":
            return False
        elif decision in ["proceed", "proceed_with_warning"]:
            return confidence >= self.confidence_threshold
        return False