"""
Minimal test without FAISS dependencies
"""
import sys
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Test just the core functionality without FAISS
print("Testing Phase 3 core functionality...")

# Test database connection
try:
    from src.utils.postgres_connection import get_connection
    conn = get_connection()
    print("✅ Database connection successful")
    
    # Test a simple query
    df = pd.read_sql_query("SELECT * FROM sales_fact_view LIMIT 5", conn)
    print(f"✅ Sample query successful. Found {len(df)} rows")
except Exception as e:
    print(f"❌ Database error: {e}")

# Test OpenAI client - FIXED
try:
    from src.llm.openai_client import OpenAIClient  # uppercase 'O'
    client = OpenAIClient()  # uppercase 'O'
    print("✅ OpenAI client initialized")
    
    # Simple test
    test_prompt = "What is 2+2?"
    response = client.generate(test_prompt)
    print(f"✅ LLM test: {response[:50]}...")
except Exception as e:
    print(f"❌ OpenAI error: {e}")
    import traceback
    traceback.print_exc()

# Test metadata catalog - FIXED
try:
    from src.vector_db.metadata_catalog import MetadataCatalog
    catalog = MetadataCatalog()
    # FIX: Changed from get_all_datasets() to get_all_views()
    views = catalog.get_all_views()  # ← CORRECT METHOD NAME
    print(f"✅ Metadata catalog loaded. Views: {views}")
except Exception as e:
    print(f"❌ Metadata catalog error: {e}")
    import traceback
    traceback.print_exc()

# Test agents (without FAISS)
try:
    # Create simple versions
    from src.agents.data_query import DataQueryAgent
    from src.agents.validation_agent import ValidationAgent
    from src.agents.narrator import NarratorAgent
    
    # Initialize agents
    data_agent = DataQueryAgent()
    validator = ValidationAgent()
    narrator = NarratorAgent()
    
    print("✅ Core agents initialized successfully")
    
    # Test with a simple intent
    test_intent = {
        "dataset": "amazon_sales",
        "intent_type": "aggregate",
        "metrics": ["sales_amount"],
        "dimensions": ["category"],
        "filters": {}
    }
    
    # Validate intent
    validation = validator.validate_intent(test_intent)
    print(f"✅ Intent validation: {validation.get('decision')}")
    
    if validation.get('valid'):
        # Execute query
        result = data_agent.execute(test_intent)
        print(f"✅ Data query: {result.get('row_count', 0)} rows")
        
        if result.get('success'):
            # Generate narration
            narration = narrator.narrate(
                "Test query", 
                test_intent, 
                result, 
                validation
            )
            print(f"✅ Narration generated: {narration[:100]}...")
    
except Exception as e:
    print(f"❌ Agent error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("Phase 3 Core Test Complete!")
print("="*60)