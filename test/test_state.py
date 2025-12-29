"""Test script for Agent State"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.state import AgentState, create_initial_state


def test_state():
    """Test Agent State functionality"""
    print("=" * 60)
    print("Testing Agent State")
    print("=" * 60)
    
    try:
        # Test initial state creation
        print("\n1. Testing initial state creation...")
        query = "Count orders"
        state = create_initial_state(query)
        print(f"   [OK] Initial state created")
        print(f"   Query: {state['query']}")
        print(f"   Messages count: {len(state['messages'])}")
        
        # Test state structure
        print("\n2. Testing state structure...")
        required_fields = [
            'messages', 'query', 'sql_query', 'query_result',
            'insights', 'visualization_spec', 'error', 'query_metadata'
        ]
        for field in required_fields:
            if field in state:
                print(f"   [OK] Field '{field}' present")
            else:
                print(f"   [FAIL] Field '{field}' missing")
                return False
        
        # Test state updates
        print("\n3. Testing state updates...")
        state['sql_query'] = "SELECT COUNT(*) FROM orders"
        state['query_metadata'] = {"type": "count", "complexity": "simple"}
        print(f"   [OK] State updated successfully")
        print(f"   SQL query: {state['sql_query']}")
        print(f"   Metadata: {state['query_metadata']}")
        
        # Test type hints
        print("\n4. Testing type hints...")
        from typing import get_type_hints
        hints = get_type_hints(AgentState)
        print(f"   [OK] Type hints available")
        print(f"   Found {len(hints)} type hints")
        
        print("\n" + "=" * 60)
        print("[OK] Agent State: ALL TESTS PASSED")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n[FAIL] Agent State: TEST FAILED")
        print(f"   Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_state()
    sys.exit(0 if success else 1)

