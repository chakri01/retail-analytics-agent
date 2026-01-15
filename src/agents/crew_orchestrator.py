"""
Crew orchestrator to coordinate all 4 agents
"""
from typing import Dict, Any
import json
import pandas as pd
from src.agents.intent_resolver import IntentResolverAgent
from src.agents.data_query import DataQueryAgent
from src.agents.validation_agent import ValidationAgent
from src.agents.narrator import NarratorAgent
from src.vector_db.faiss_index import FaissIndex
from src.vector_db.metadata_catalog import MetadataCatalog

class CrewOrchestrator:
    """Orchestrates the 4-agent crew for retail analytics"""
    
    def __init__(self):
        # Initialize all agents
        self.intent_resolver = IntentResolverAgent()
        self.data_query = DataQueryAgent()
        self.validator = ValidationAgent()
        self.narrator = NarratorAgent()
        
        # Initialize FAISS index
        self.faiss_index = FaissIndex()
        self.metadata_catalog = MetadataCatalog()
        
        # Build FAISS index if it doesn't exist
        if not self.faiss_index.exists():
            print("Building FAISS index from metadata...")
            chunks = self.metadata_catalog.text_chunks
            metadata = [{"chunk_id": i} for i in range(len(chunks))]
            self.faiss_index.build_index(chunks, metadata)
        else:
            print("Loading existing FAISS index...")
    
    def process_query(self, user_query: str, dataset: str = None) -> Dict[str, Any]:
        """Process user query through all 4 agents"""
        
        print(f"\n{'='*60}")
        print(f"Processing query: {user_query}")
        print(f"{'='*60}")
        
        # Step 1: Intent Resolution
        dataset_context = f"Focus on {dataset}" if dataset else None
        intent = self.intent_resolver.resolve(user_query, dataset_context)
        
        print(f"Intent resolved: {json.dumps(intent, indent=2)}")
        
        # Override dataset if specified
        if dataset and intent.get("dataset") != dataset:
            intent["dataset"] = dataset
            print(f"Dataset overridden to: {dataset}")
        
        # Step 2: Validate Intent
        intent_validation = self.validator.validate_intent(intent)
        print(f"Intent validation: {intent_validation}")
        
        if not self.validator.should_proceed(intent_validation):
            return {
                "success": False,
                "stage": "intent_validation",
                "error": intent_validation.get("reason", "Intent validation failed"),
                "intent": intent,
                "validation": intent_validation,
                "data": None,
                "narration": None,
                "should_clarify": intent_validation.get("decision") == "clarify"
            }
        
        # Step 3: Data Query
        query_result = self.data_query.execute(intent)
        
        if not query_result.get("success", False):
            return {
                "success": False,
                "stage": "data_query",
                "error": query_result.get("error", "Query execution failed"),
                "intent": intent,
                "validation": intent_validation,
                "data": None,
                "narration": None,
                "should_clarify": False
            }
        
        print(f"Query executed. Rows: {query_result.get('row_count')}")
        print(f"SQL: {query_result.get('sql', '')[:100]}...")
        
        # Step 4: Validate Results
        result_validation = self.validator.validate_results(query_result)
        print(f"Result validation: {result_validation}")
        
        if not self.validator.should_proceed(result_validation):
            return {
                "success": False,
                "stage": "result_validation",
                "error": result_validation.get("reason", "Result validation failed"),
                "intent": intent,
                "validation": result_validation,
                "data": query_result.get("data"),
                "narration": None,
                "should_clarify": result_validation.get("decision") == "clarify"
            }
        
        # Step 5: Generate Narration
        narration = self.narrator.narrate(user_query, intent, query_result, result_validation)
        
        print(f"Narration generated ({len(narration)} chars)")
        
        # Return successful result
        return {
            "success": True,
            "stage": "complete",
            "intent": intent,
            "validation": {
                "intent": intent_validation,
                "results": result_validation
            },
            "data": query_result.get("data"),
            "row_count": query_result.get("row_count"),
            "columns": query_result.get("columns"),
            "sql": query_result.get("sql"),
            "narration": narration
        }
    
    def get_summary(self, dataset: str = "amazon_sales") -> Dict[str, Any]:
        """Generate pre-defined summary for a dataset"""
        summary_queries = {
            "amazon_sales": [
                "What are total sales by category?",
                "What are monthly sales trends?",
                "Which countries have the highest sales?",
                "Top 10 products by sales"
            ],
            "inventory": [
                "Current stock by category",
                "Low stock items count",
                "Inventory status summary"
            ]
        }
        
        queries = summary_queries.get(dataset, [])
        results = []
        
        for query in queries[:3]:  # Limit to 3 queries
            result = self.process_query(query, dataset)
            if result["success"]:
                results.append({
                    "query": query,
                    "narration": result["narration"],
                    "row_count": result.get("row_count", 0)
                })
        
        return {
            "dataset": dataset,
            "summary_items": results,
            "total_queries": len(results)
        }