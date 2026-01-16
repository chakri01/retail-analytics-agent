"""
Unified Crew orchestrator for cross-dataset queries
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
    """Orchestrates agents for cross-dataset queries"""
    
    def __init__(self):
        # Initialize unified agents
        self.intent_resolver = IntentResolverAgent()
        self.data_query = DataQueryAgent()
        self.validator = ValidationAgent()
        self.narrator = NarratorAgent()

    def _prepare_data_for_narration(self, data_result: Dict) -> Dict:
       """Convert non-serializable objects in data result"""
       if not data_result.get('success'):
           return data_result
       
       # Convert data to serializable format
       serializable_data = []
       for row in data_result.get('data', []):
           serializable_row = {}
           for key, value in row.items():
               # Convert Timestamp to string
               if hasattr(value, 'isoformat'):  # Handles Timestamp, datetime, etc.
                   serializable_row[key] = value.isoformat()
               elif pd.isna(value):  # Handle NaN values
                   serializable_row[key] = None
               else:
                   serializable_row[key] = value
           serializable_data.append(serializable_row)
       
       # Return updated result
       result_copy = data_result.copy()
       result_copy['data'] = serializable_data
       return result_copy
    
    def _prepare_data_for_narration(self, data_result: Dict) -> Dict:
        """Convert non-serializable objects in data result"""
        if not data_result.get('success'):
            return data_result
        
        # Convert data to serializable format
        serializable_data = []
        for row in data_result.get('data', []):
            serializable_row = {}
            for key, value in row.items():
                # Convert Timestamp to string
                if hasattr(value, 'isoformat'):  # Handles Timestamp, datetime, etc.
                    serializable_row[key] = value.isoformat()
                else:
                    serializable_row[key] = value
            serializable_data.append(serializable_row)
        
        # Return updated result
        result_copy = data_result.copy()
        result_copy['data'] = serializable_data
        return result_copy

    def process_query(self, user_query: str) -> Dict:
     """Process a user query through the agent crew"""
     
     # 1. Resolve intent
     print(f"\n{'='*60}")
     print(f"Processing unified query: {user_query}")
     print('='*60)
     
     intent = self.intent_resolver.resolve(user_query)
     print(f"Intent resolved: {json.dumps(intent, indent=2)}")
     
     # 2. Validate intent
     intent_validation = self.validator.validate_intent(intent)
     print(f"Intent validation: {intent_validation}")
     
     # Check if we should proceed
     if not self.validator.should_proceed(intent_validation):
         print(f"❌ Failed at stage: intent_validation")
         print(f"💡 Error: {intent_validation.get('reason', 'Unknown error')}")
         return {
             "success": False,
             "stage": "intent_validation",
             "error": intent_validation.get('reason', 'Validation failed'),
             "validation": intent_validation
         }
     
     # 3. Execute data query
     try:
         query_result = self.data_query.execute(intent)
         
         if not query_result.get('success', False):
             print(f"❌ Failed at stage: data_query")
             print(f"💡 Error: {query_result.get('error', 'Unknown error')}")
             return {
                 "success": False,
                 "stage": "data_query",
                 "error": query_result.get('error', 'Query failed'),
                 "data": query_result
             }
         
         # 4. Validate results
         result_validation = self.validator.validate_results(query_result)
         
         # 5. Generate narration (with prepared data)
         prepared_result = self._prepare_data_for_narration(query_result)

         narration = self.narrator.narrate(
                user_query=user_query,
                intent=intent,
                query_result=prepared_result,  # Fixed parameter name
                validation_result=result_validation  # Fixed parameter name
            )
         
         # 6. Return final result
         return {
            "success": True,
            "query": user_query,
            "intent": intent,
            "validation": result_validation,
            "data": prepared_result.get('data', []),  # Use prepared data for JSON
            "row_count": query_result.get('row_count', 0),
            "columns": query_result.get('columns', []),
            "sql": query_result.get('sql', ''),
            "narration": narration
        }
         
     except Exception as e:
         print(f"❌ Failed at stage: execution")
         print(f"💡 Error: {e}")
         import traceback
         traceback.print_exc()
         return {
             "success": False,
             "stage": "execution",
             "error": str(e)
         }
    