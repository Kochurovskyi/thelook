"""End-to-end test for complete workflow"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.graph import run_agent


def test_e2e():
    """End-to-end test with various query types"""
    print("=" * 60)
    print("End-to-End Testing")
    print("=" * 60)
    
    test_queries = [
        ("Count orders", "count"),
        ("Show top 10 products by revenue", "ranking"),
        ("What is the average order value?", "aggregation"),
        ("How many users are there?", "count"),
    ]
    
    results = []
    
    for query, expected_type in test_queries:
        print(f"\n{'='*60}")
        print(f"Testing: {query}")
        print(f"Expected type: {expected_type}")
        print(f"{'='*60}")
        
        try:
            final_state = run_agent(query)
            
            success = True
            issues = []
            
            if final_state.get("error"):
                success = False
                issues.append(f"Error: {final_state['error']}")
            
            if not final_state.get("sql_query"):
                success = False
                issues.append("No SQL generated")
            
            query_result = final_state.get("query_result")
            if query_result is None or (hasattr(query_result, 'empty') and query_result.empty):
                success = False
                issues.append("No results returned")
            
            if success:
                df = final_state["query_result"]
                print(f"   [OK] Query processed successfully")
                print(f"   SQL: {final_state['sql_query'][:80]}...")
                print(f"   Results: {len(df)} rows, {len(df.columns)} columns")
                if final_state.get("insights"):
                    print(f"   Insights: {final_state['insights'][:100]}...")
                results.append(("PASS", query))
            else:
                print(f"   [FAIL] Issues: {', '.join(issues)}")
                results.append(("FAIL", query))
                
        except Exception as e:
            print(f"   [FAIL] Exception: {str(e)}")
            results.append(("FAIL", query))
            import traceback
            traceback.print_exc()
    
    # Summary
    print(f"\n{'='*60}")
    print("Test Summary")
    print(f"{'='*60}")
    
    passed = sum(1 for r in results if r[0] == "PASS")
    total = len(results)
    
    for status, query in results:
        print(f"   [{status}] {query}")
    
    print(f"\n   Passed: {passed}/{total}")
    
    if passed == total:
        print("\n[OK] End-to-End: ALL TESTS PASSED")
        return True
    else:
        print(f"\n[FAIL] End-to-End: {total - passed} test(s) failed")
        return False


if __name__ == "__main__":
    success = test_e2e()
    sys.exit(0 if success else 1)

