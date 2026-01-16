"""
Agent 1: Intent Resolver Agent (OpenAI)
Responsibility: Convert user question → structured intent JSON
"""
import json
from typing import Dict, Any
from src.llm.openai_client import OpenAIClient
from src.llm.prompts import INTENT_RESOLVER_SYSTEM_PROMPT
from src.vector_db.metadata_catalog import MetadataCatalog
from src.vector_db.faiss_index import FaissIndex

class IntentResolverAgent:
    """Agent 1: Convert user question to structured intent for unified queries"""
    
    def __init__(self):
        self.llm = OpenAIClient()
        self.metadata = MetadataCatalog()
        self.faiss_index = FaissIndex()
        
        # Build FAISS index if not exists
        if not self.faiss_index.exists():
            print("Building FAISS index from unified metadata...")
            chunks = self.metadata.text_chunks
            self.faiss_index.build_index(chunks)
    
    def resolve(self, user_query: str) -> Dict[str, Any]:
        """Resolve user query to structured intent for unified data"""
        # Get relevant context from FAISS
        faiss_context = self.faiss_index.get_relevant_context(user_query)
        
        # Prepare context about the unified data model
        context = self._build_unified_context(faiss_context)
        
        full_prompt = f"{context}\n\nUser Question: {user_query}"
        
        # Get structured intent from LLM
        intent = self.llm.generate_structured(
            full_prompt, 
            self._get_unified_system_prompt()
        )
        
        # Log intent
        self._log_intent(user_query, intent)
        
        return intent
    
    def _build_unified_context(self, faiss_context: str) -> str:
        """Build context about the unified data model"""
        context = ""
        
        if faiss_context:
            context += f"Relevant context from knowledge base:\n{faiss_context}\n\n"
        
        # Describe the unified data model
        context += "UNIFIED DATA MODEL OVERVIEW:\n"
        context += "The system has 3 main views that are all related:\n\n"
        
        for view_name, info in self.metadata.views.items():
            context += f"1. {view_name}: {info['description']}\n"
            context += f"   Key columns: {', '.join(list(info['columns'].keys())[:5])}...\n"
        
        context += "\nVIEW RELATIONSHIPS:\n"
        context += "- All views can be joined using the 'sku' column\n"
        context += "- sales_fact_view contains transaction data\n"
        context += "- product_dim_view contains product master data\n"
        context += "- inventory_dim_view contains current stock levels\n"
        
        context += "\nCOMMON BUSINESS QUESTIONS YOU CAN ANSWER:\n"
        for i, query in enumerate(self.metadata.common_queries[:5], 1):
            context += f"{i}. {query}\n"
        
        return context
    
    def _get_unified_system_prompt(self) -> str:
        """Get system prompt for unified queries"""
        return """
        You are an Intent Resolver for a unified retail analytics system.
        
        Available Views (all related by sku column):
        1. sales_fact_view: Sales transactions across all channels
        2. product_dim_view: Master product catalog
        3. inventory_dim_view: Current inventory levels
        
        You can answer complex questions that span multiple views.
        
        Examples of cross-view questions:
        - "Which products are low in stock but high in sales?" → Needs inventory + sales
        - "What is the average order value by product category?" → Needs sales + products
        - "Show me products that are out of stock" → Needs inventory + products
        
        Intent types for cross-view queries:
        - aggregate: Summarize metrics (can be from multiple views)
        - compare: Compare metrics across dimensions
        - trend: Analyze over time
        - top: Get top N items
        - join: Explicitly join multiple views
        
        Return ONLY JSON like:
        {
            "dataset": "sales_fact_view",
            "intent_type": "aggregate",
            "metrics": ["amount"],
            "dimensions": ["category"],
            "filters": {},
            "needed_views": ["sales_fact_view", "product_dim_view"]
        }
        
        Rules:
        1. Always include a "dataset" field - use the main fact table as primary dataset
        2. For sales queries → dataset: "sales_fact_view"
        3. For inventory queries → dataset: "inventory_dim_view"
        4. For product queries → dataset: "product_dim_view"
        5. If multiple views equally important → use the first needed_view as dataset
        6. Include all views needed in "needed_views"
        7. Only use columns that exist in the views
        8. For filters, specify the view if ambiguous
        9. If unclear → set intent_type to "clarify"
        """
    
    def _log_intent(self, query: str, intent: Dict[str, Any]):
        """Log intent resolution"""
        log_entry = {
            "query": query,
            "intent": intent,
            "timestamp": "2024-01-01 00:00:00"
        }
        
        # Append to log file
        with open("logs/intent_resolution.log", "a") as f:
            f.write(json.dumps(log_entry) + "\n")