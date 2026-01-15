"""
Main entry point for agent
"""
import sys
import os
import argparse
from src.agents.crew_orchestrator import CrewOrchestrator
from src.utils.logging_config import agentLogger

def test_agent():
    """Test the agent system"""
    print("="*60)
    print("agent: GenAI Assistant - System Test")
    print("="*60)
    
    # Initialize orchestrator
    orchestrator = CrewOrchestrator()
    logger = agentLogger()
    
    # Test queries
    test_queries = [
        "What are total sales by category?",
        "Show me top 5 products by sales",
        "What is inventory status by category?",
        "Which country has the highest sales?"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Testing: '{query}'")
        print("-"*40)
        
        result = orchestrator.process_query(query)
        
        if result["success"]:
            print(f"✅ Success!")
            print(f"📊 Rows: {result['row_count']}")
            print(f"🤖 Narration: {result['narration'][:150]}...")
            print(f"📝 Intent: {result['intent'].get('intent_type')}")
            
            # Log the result
            logger.log_intent(query, result["intent"])
            if result.get("sql"):
                logger.log_sql(result["sql"], result["intent"])
            
        else:
            print(f"❌ Failed at stage: {result['stage']}")
            print(f"💡 Error: {result['error']}")
    
    print("\n" + "="*60)
    print("✅ agent system test complete!")
    print("🎯 To run the UI: streamlit run src/ui/streamlit_app.py")
    print("="*60)

def main():
    parser = argparse.ArgumentParser(description="agent: GenAI Assistant")
    parser.add_argument("--test", action="store_true", help="Run system tests")
    parser.add_argument("--query", type=str, help="Run a single query")
    parser.add_argument("--dataset", type=str, default="amazon_sales", help="Dataset to query")
    
    args = parser.parse_args()
    
    if args.test:
        test_agent()
    elif args.query:
        orchestrator = CrewOrchestrator()
        result = orchestrator.process_query(args.query, args.dataset)
        print(json.dumps(result, indent=2))
    else:
        print("Please specify a command:")
        print("  --test                Run system tests")
        print("  --query 'your query'  Run a single query")
        print("\nTo run the UI:")
        print("  streamlit run src/ui/streamlit_app.py")

if __name__ == "__main__":
    main()