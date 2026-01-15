"""
Agent 4: Insight Narrator Agent (OpenAI)
Responsibility: Convert numbers → business-friendly text
"""
from typing import Dict, Any, List
from src.llm.openai_client import OpenAIClient  
from src.llm.prompts import NARRATOR_SYSTEM_PROMPT
import json

class NarratorAgent:
    """Agent 4: Convert data results to business-friendly text"""
    
    def __init__(self):
        self.llm = OpenAIClient() 
    
    def narrate(self, user_query: str, intent: Dict[str, Any], 
                query_result: Dict[str, Any], validation_result: Dict[str, Any]) -> str:
        """Generate narrative from results"""
        
        if not query_result.get("success", False):
            return "I couldn't retrieve the data for your question. Please try rephrasing or check if the data exists."
        
        data = query_result.get("data", [])
        row_count = query_result.get("row_count", 0)
        
        if row_count == 0:
            return "No data found matching your criteria. Try broadening your search or checking different filters."
        
        # If validation has warnings, mention them
        narration_parts = []
        if validation_result.get("decision") == "proceed_with_warning":
            warning = validation_result.get("reason", "")
            if warning:
                narration_parts.append(f"Note: {warning}")
        
        # Prepare data for narration
        if len(data) > 10:
            # Truncate large datasets for the prompt
            data_sample = data[:10]
            data_str = json.dumps(data_sample, indent=2)
            data_str += f"\n... and {len(data) - 10} more rows"
        else:
            data_str = json.dumps(data, indent=2)
        
        # Create prompt
        prompt = f"""
        Original Question: {user_query}
        
        Intent: {json.dumps(intent, indent=2)}
        
        Data Results ({row_count} rows):
        {data_str}
        
        Please provide a concise business insight based ONLY on the data above.
        """
        
        try:
            narration = self.llm.generate(prompt, NARRATOR_SYSTEM_PROMPT)
            
            # Add data disclaimer
            if len(data) > 20:
                disclaimer = f"\n\nBased on {row_count} records. For detailed analysis, download the full dataset."
                narration += disclaimer
            
            return narration
            
        except Exception as e:
            print(f"Narration error: {e}")
            # Fallback narration
            return self._fallback_narration(data, intent, row_count)
    
    def _fallback_narration(self, data: List[Dict], intent: Dict[str, Any], row_count: int) -> str:
        """Fallback narration when LLM fails"""
        dataset = intent.get("dataset", "data")
        metrics = intent.get("metrics", [])
        dimensions = intent.get("dimensions", [])
        
        if not data:
            return f"No {dataset} data found."
        
        # Simple statistical summary
        if metrics and dimensions:
            # Grouped data
            summary = f"Found {row_count} records of {dataset} data grouped by {', '.join(dimensions)}. "
            
            # Add metric summary if possible
            if metrics and data:
                first_metric = metrics[0]
                if first_metric in data[0]:
                    values = [row.get(first_metric, 0) for row in data]
                    if values:
                        avg_val = sum(values) / len(values)
                        max_val = max(values)
                        summary += f"The {first_metric} ranges from {min(values):.2f} to {max_val:.2f} with an average of {avg_val:.2f}."
            
            return summary
        
        # Simple data summary
        return f"Retrieved {row_count} records from {dataset}. The data includes metrics like {', '.join(metrics[:2])}."