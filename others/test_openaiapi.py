"""
Test the fixed OpenAI setup
"""
import os
from dotenv import load_dotenv

load_dotenv()

print("Testing fixed OpenAI setup...")
print("="*60)

# Check API key
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("❌ OPENAI_API_KEY not found in .env")
    print("   Please add: OPENAI_API_KEY=your_key_here")
    exit(1)
else:
    print(f"✅ API Key found (starts with: {api_key[:10]}...)")

# Test OpenAI client
try:
    from src.llm.openai_client import OpenAIClient
    client = OpenAIClient(model_name="gpt-3.5-turbo")
    
    if client.client:
        print("✅ OpenAI client created successfully")
        
        # Test simple generation
        print("\nTesting simple generation...")
        response = client.generate("Say hello in one word")
        print(f"   Response: {response}")
        
        # Test structured generation
        print("\nTesting structured generation...")
        intent = client.generate_structured(
            "What are total sales by category?",
            "You are an intent resolver. Return JSON with dataset, intent_type, metrics, dimensions, filters."
        )
        print(f"   Intent: {intent}")
        
        print("\n" + "="*60)
        print("✅ OpenAI is working perfectly!")
        print("="*60)
        
    else:
        print("❌ OpenAI client failed to initialize")
        print("   Using mock mode for now")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()