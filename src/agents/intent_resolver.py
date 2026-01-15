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
    """Agent 1: Convert user question to structured intent JSON"""
    
    def __init__(self):
        self.llm = OpenAIClient()
        self.metadata_catalog = MetadataCatalog()
        self.faiss_index = FaissIndex()
    
    def resolve(self, user_query: str, dataset_context: str = None) -> Dict[str, Any]:
        """Resolve user query to structured intent"""
        # Get relevant context from FAISS
        faiss_context = self.faiss_index.get_relevant_context(user_query)
        
        # Prepare context
        context = ""
        if faiss_context:
            context += f"Relevant context from knowledge base:\n{faiss_context}\n\n"
        
        if dataset_context:
            context += f"Dataset context: {dataset_context}\n\n"
        
        # Add available datasets summary
        datasets = self.metadata_catalog.get_all_datasets()
        context += f"Available datasets: {', '.join(datasets)}\n"
        
        for dataset in datasets[:2]:  # Show first 2 datasets
            info = self.metadata_catalog.get_dataset_info(dataset)
            context += f"- {dataset}: {info['description'][:100]}...\n"
        
        full_prompt = f"{context}\nUser Question: {user_query}"
        
        # Get structured intent from LLM
        intent = self.llm.generate_structured(full_prompt, INTENT_RESOLVER_SYSTEM_PROMPT)
        
        # Log intent
        self._log_intent(user_query, intent)
        
        return intent
    
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