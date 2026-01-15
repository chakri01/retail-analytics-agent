"""
OpenAI LLM wrapper - interchangeable LLM interface
"""
import os
from typing import List, Dict, Any
from openai import OpenAI
from dotenv import load_dotenv
import json

load_dotenv()

class OpenAIClient:
    """Wrapper for OpenAI LLM with centralized configuration"""
    
    def __init__(self, model_name="gpt-3.5-turbo", temperature=0.1):
        self.model_name = model_name
        self.temperature = temperature
        self.api_key = os.getenv("OPENAI_API_KEY") or os.getenv("GEMINI_API_KEY")
        
        if not self.api_key:
            print("⚠️  OPENAI_API_KEY not found in .env file")
            print("   Using mock responses for testing")
            self.client = None
        else:
            try:
                self.client = OpenAI(api_key=self.api_key)
                # Test the connection
                test_response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": "Say hello"}],
                    max_tokens=10
                )
                print(f"✅ OpenAI client initialized with model: {model_name}")
            except Exception as e:
                print(f"❌ OpenAI initialization error: {e}")
                print("   Using mock responses")
                self.client = None
    
    def generate(self, prompt: str, system_prompt: str = None) -> str:
        """Generate response from OpenAI"""
        if not self.client:
            return self._mock_response(prompt, system_prompt)
        
        try:
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})
            
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                temperature=self.temperature,
                max_tokens=500
            )
            
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"OpenAI generation error: {e}")
            return self._mock_response(prompt, system_prompt)
    
    def generate_structured(self, prompt: str, system_prompt: str) -> Dict[str, Any]:
        """Generate structured JSON response"""
        if not self.client:
            return self._mock_intent(prompt)
        
        try:
            structured_prompt = f"""
            {system_prompt}
            
            User Query: {prompt}
            
            Return ONLY valid JSON. No other text.
            """
            
            messages = [
                {"role": "system", "content": "You are a JSON output generator. Always return valid JSON."},
                {"role": "user", "content": structured_prompt}
            ]
            
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                temperature=0.1,  # Lower temp for structured output
                max_tokens=500,
                response_format={"type": "json_object"}
            )
            
            result_text = response.choices[0].message.content.strip()
            return json.loads(result_text)
            
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Raw response: {result_text if 'result_text' in locals() else 'No response'}")
            return self._mock_intent(prompt)
        except Exception as e:
            print(f"Structured generation error: {e}")
            return self._mock_intent(prompt)
    
    def _mock_intent(self, query: str) -> Dict[str, Any]:
        """Generate mock intent for testing"""
        query_lower = query.lower()
        
        if "sales" in query_lower and "category" in query_lower:
            return {
                "dataset": "amazon_sales",
                "intent_type": "aggregate",
                "metrics": ["sales_amount"],
                "dimensions": ["category"],
                "filters": {}
            }
        elif "top" in query_lower and ("product" in query_lower or "items" in query_lower):
            top_n = 5
            if "top 3" in query_lower:
                top_n = 3
            elif "top 10" in query_lower:
                top_n = 10
            return {
                "dataset": "amazon_sales",
                "intent_type": "top",
                "metrics": ["sales_amount"],
                "dimensions": ["product_name"],
                "filters": {"top_n": top_n}
            }
        elif "inventory" in query_lower:
            return {
                "dataset": "inventory",
                "intent_type": "aggregate",
                "metrics": ["current_stock"],
                "dimensions": ["category_clean"],
                "filters": {}
            }
        elif "country" in query_lower or "india" in query_lower or "us" in query_lower:
            return {
                "dataset": "amazon_sales",
                "intent_type": "aggregate",
                "metrics": ["sales_amount"],
                "dimensions": ["country"],
                "filters": {}
            }
        else:
            return {
                "dataset": "amazon_sales",
                "intent_type": "aggregate",
                "metrics": ["sales_amount"],
                "dimensions": ["category"],
                "filters": {}
            }
    
    def _mock_response(self, prompt: str, system_prompt: str = None) -> str:
        """Generate mock response for testing"""
        if "sales" in prompt.lower():
            return "Based on the sales data, Electronics has the highest total sales at $50,000, followed by Clothing at $30,000, and Home Goods at $20,000."
        elif "inventory" in prompt.lower():
            return "Current inventory shows 1,200 units in stock across all categories, with 150 items marked as low stock."
        elif "top" in prompt.lower():
            return "The top selling products are: Product A ($15,000), Product B ($12,000), Product C ($10,000)."
        else:
            return "This is a mock response. Please set OPENAI_API_KEY in your .env file to use the actual AI."