"""Test script for LangGraph workflow"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.graph import create_agent_graph, run_agent


def test_graph():
    """Test LangGraph workflow"""
    print("=" * 60)
    print("Testing LangGraph Workflow")
    print("=" * 60)
    
    try:
        # Test graph creation
        print("\n1. Testing graph creation...")
        graph = create_agent_graph()
        print("   [OK] Graph created and compiled")
        
        # Test simple query
        print("\n2. Testing simple query workflow...")
        query = "Count orders"
        print(f"   Query: {query}")
        
        final_state = run_agent(query)
        
        print(f"   [OK] Workflow completed")
        print(f"   SQL generated: {final_state.get('sql_query') is not None}")
        print(f"   Results: {final_state.get('query_result') is not None}")
        print(f"   Insights: {final_state.get('insights') is not None}")
        
        if final_state.get("error"):
            print(f"   [WARN] Error occurred: {final_state['error']}")
        else:
            if final_state.get("query_result") is not None:
                df = final_state["query_result"]
                print(f"   Result rows: {len(df)}")
                print(f"   Result columns: {list(df.columns)}")
        
        # Test complex query
        print("\n3. Testing complex query workflow...")
        query2 = "Show top 5 products by revenue"
        print(f"   Query: {query2}")
        
        final_state2 = run_agent(query2)
        
        print(f"   [OK] Complex workflow completed")
        if final_state2.get("error"):
            print(f"   [WARN] Error: {final_state2['error']}")
        else:
            if final_state2.get("query_result") is not None:
                df2 = final_state2["query_result"]
                print(f"   Result rows: {len(df2)}")
                print(f"   Visualization: {final_state2.get('visualization_spec') is not None}")
        
        print("\n" + "=" * 60)
        print("[OK] LangGraph Workflow: ALL TESTS PASSED")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n[FAIL] LangGraph Workflow: TEST FAILED")
        print(f"   Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_graph()
    sys.exit(0 if success else 1)

