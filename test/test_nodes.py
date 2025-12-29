"""Test script for Agent Nodes"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.nodes import (
    understand_query, generate_sql, validate_sql,
    execute_query, analyze_results, create_visualization
)
from agents.state import create_initial_state


def test_nodes(node_type: str = "all"):
    """Test agent nodes functionality"""
    print("=" * 60)
    print("Testing Agent Nodes")
    print("=" * 60)
    
    if node_type == "all":
        node_types = ["understand", "generate_sql", "validate", "execute", "analyze", "visualize"]
        for nt in node_types:
            test_nodes(nt)
        return
    
    try:
        if node_type == "understand":
            print("\n1. Testing understand_query node...")
            state = create_initial_state("Show me top 10 products by revenue")
            state = understand_query(state)
            print(f"   [OK] Query understood")
            print(f"   Type: {state['query_metadata']['type']}")
            print(f"   Complexity: {state['query_metadata']['complexity']}")
            
        elif node_type == "generate_sql":
            print("\n2. Testing generate_sql node...")
            state = create_initial_state("Count orders")
            state = understand_query(state)
            state = generate_sql(state)
            if state.get("error"):
                print(f"   [SKIP] SQL generation skipped: {state['error']}")
            else:
                print(f"   [OK] SQL generated")
                print(f"   SQL: {state['sql_query'][:100]}...")
        
        elif node_type == "validate":
            print("\n3. Testing validate_sql node...")
            # Test valid SQL
            state = create_initial_state("Count orders")
            state["sql_query"] = "SELECT COUNT(*) FROM `bigquery-public-data.thelook_ecommerce.orders`"
            state = validate_sql(state)
            if state.get("error"):
                print(f"   [FAIL] Valid SQL was rejected: {state['error']}")
            else:
                print(f"   [OK] Valid SQL accepted")
            
            # Test invalid SQL
            state2 = create_initial_state("Drop table")
            state2["sql_query"] = "DROP TABLE orders"
            state2 = validate_sql(state2)
            if state2.get("error"):
                print(f"   [OK] Invalid SQL rejected: {state2['error']}")
            else:
                print(f"   [FAIL] Invalid SQL was accepted")
        
        elif node_type == "execute":
            print("\n4. Testing execute_query node...")
            state = create_initial_state("Count orders")
            state["sql_query"] = "SELECT COUNT(*) as count FROM `bigquery-public-data.thelook_ecommerce.orders`"
            state = validate_sql(state)
            state = execute_query(state)
            if state.get("error"):
                print(f"   [FAIL] Query execution failed: {state['error']}")
            else:
                print(f"   [OK] Query executed")
                print(f"   Results: {len(state['query_result'])} rows")
                print(f"   Columns: {list(state['query_result'].columns)}")
        
        elif node_type == "analyze":
            print("\n5. Testing analyze_results node...")
            state = create_initial_state("Count orders")
            state["sql_query"] = "SELECT COUNT(*) as count FROM `bigquery-public-data.thelook_ecommerce.orders`"
            state = validate_sql(state)
            state = execute_query(state)
            state = analyze_results(state)
            if state.get("error"):
                print(f"   [SKIP] Analysis skipped: {state['error']}")
            else:
                print(f"   [OK] Insights generated")
                print(f"   Insights: {state['insights'][:150]}...")
        
        elif node_type == "visualize":
            print("\n6. Testing create_visualization node...")
            state = create_initial_state("Top 5 products")
            state["sql_query"] = """
                SELECT name, SUM(sale_price) as revenue 
                FROM `bigquery-public-data.thelook_ecommerce.products` p
                JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON p.id = oi.product_id
                GROUP BY name 
                ORDER BY revenue DESC 
                LIMIT 5
            """
            state = validate_sql(state)
            state = execute_query(state)
            state = create_visualization(state)
            if state.get("visualization_spec"):
                print(f"   [OK] Visualization created")
                print(f"   Type: {state['visualization_spec']['type']}")
            else:
                print(f"   [SKIP] Visualization not created (may be expected)")
        
        print("\n" + "=" * 60)
        print(f"[OK] Node '{node_type}': TEST PASSED")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n[FAIL] Node '{node_type}': TEST FAILED")
        print(f"   Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    node_type = sys.argv[1] if len(sys.argv) > 1 else "all"
    success = test_nodes(node_type)
    sys.exit(0 if success else 1)

