"""Sanity checks for Phase 3: Advanced Features"""
import sys
import os
import pytest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.cache_service import CacheService
from utils.query_optimizer import QueryOptimizer
from agents.specialized_agents import route_to_agent
from agents.state import create_initial_state
from prompts.business_insights import get_insight_template_for_type
from services.bigquery_service import BigQueryService
import pandas as pd


def test_cache_service():
    """Sanity check for CacheService"""
    print("=" * 60)
    print("Testing Cache Service")
    print("=" * 60)
    
    try:
        cache = CacheService(ttl_seconds=60)
        print("[OK] Cache service initialized")
        
        # Test caching
        df = pd.DataFrame({'test': [1, 2, 3]})
        cache.cache_result("SELECT * FROM test", df)
        print("[OK] Result cached")
        
        # Test retrieval
        cached = cache.get_cached_result("SELECT * FROM test")
        assert cached is not None, "Cached result should not be None"
        assert len(cached) == 3, f"Expected 3 rows, got {len(cached)}"
        print("[OK] Cached result retrieved correctly")
        
        # Test stats
        stats = cache.get_cache_stats()
        print(f"[OK] Cache stats: {stats['total_entries']} entries")
        
        print("\n" + "=" * 60)
        print("[OK] Cache Service: ALL TESTS PASSED")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[FAIL] Cache Service: TEST FAILED")
        print(f"   Error: {str(e)}")
        pytest.fail(f"Cache Service test failed: {str(e)}")


def test_query_optimizer():
    """Sanity check for QueryOptimizer"""
    print("=" * 60)
    print("Testing Query Optimizer")
    print("=" * 60)
    
    try:
        query = """
        SELECT p.name, SUM(oi.sale_price) as revenue
        FROM `bigquery-public-data.thelook_ecommerce.products` p
        JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON p.id = oi.product_id
        GROUP BY p.name
        ORDER BY revenue DESC
        LIMIT 10
        """
        
        # Test cost estimation
        cost_info = QueryOptimizer.estimate_query_cost(query)
        print(f"[OK] Cost estimation: ${cost_info['estimated_cost_usd']:.6f}")
        print(f"   Complexity: {cost_info['complexity']}")
        print(f"   Tables accessed: {cost_info['tables_accessed']}")
        
        # Test optimization suggestions
        suggestions = QueryOptimizer.suggest_optimizations(query)
        print(f"[OK] Optimization suggestions: {len(suggestions)} suggestions")
        
        # Test validation
        validation = QueryOptimizer.validate_query_structure(query)
        assert validation["is_valid"], f"Query validation failed: {validation.get('errors', 'Unknown error')}"
        print("[OK] Query validation: Valid")
        
        print("\n" + "=" * 60)
        print("[OK] Query Optimizer: ALL TESTS PASSED")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[FAIL] Query Optimizer: TEST FAILED")
        print(f"   Error: {str(e)}")
        pytest.fail(f"Query Optimizer test failed: {str(e)}")


def test_specialized_agents():
    """Sanity check for Specialized Agents"""
    print("=" * 60)
    print("Testing Specialized Agents Router")
    print("=" * 60)
    
    try:
        test_cases = [
            ("Show customer segments", "customer_segmentation"),
            ("Top products by revenue", "product_performance"),
            ("Sales trends over time", "sales_trends"),
            ("Sales by location", "geographic"),
            ("Count orders", "general")
        ]
        
        for query, expected_route in test_cases:
            state = create_initial_state(query)
            route = route_to_agent(state)
            
            assert route == expected_route, f"Expected route '{expected_route}' for query '{query}', got '{route}'"
            print(f"[OK] '{query}' -> {route}")
        
        print("\n" + "=" * 60)
        print("[OK] Specialized Agents Router: ALL TESTS PASSED")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[FAIL] Specialized Agents: TEST FAILED")
        print(f"   Error: {str(e)}")
        pytest.fail(f"Specialized Agents test failed: {str(e)}")


def test_business_insights():
    """Sanity check for Business Insights"""
    print("=" * 60)
    print("Testing Business Insights")
    print("=" * 60)
    
    try:
        query_types = ["count", "ranking", "aggregation", "temporal", "customer_analysis"]
        
        for query_type in query_types:
            template = get_insight_template_for_type(query_type)
            assert template is not None, f"Template for '{query_type}' should not be None"
            assert "focus" in template, f"Template for '{query_type}' missing 'focus' key"
            print(f"[OK] Template for '{query_type}': {template['focus']}")
        
        print("\n" + "=" * 60)
        print("[OK] Business Insights: ALL TESTS PASSED")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[FAIL] Business Insights: TEST FAILED")
        print(f"   Error: {str(e)}")
        pytest.fail(f"Business Insights test failed: {str(e)}")


def test_bigquery_with_optimization():
    """Sanity check for BigQuery service with optimization features"""
    print("=" * 60)
    print("Testing BigQuery Service with Optimization")
    print("=" * 60)
    
    try:
        service = BigQueryService(enable_cache=True)
        print("[OK] BigQuery service with caching enabled")
        
        # Test cost estimation
        query = "SELECT COUNT(*) FROM `bigquery-public-data.thelook_ecommerce.orders`"
        cost_info = service.estimate_query_cost(query)
        print(f"[OK] Cost estimation available: ${cost_info['estimated_cost_usd']:.6f}")
        
        # Test cache stats
        if service.cache_service:
            stats = service.cache_service.get_cache_stats()
            print(f"[OK] Cache stats: {stats['total_entries']} entries")
        
        print("\n" + "=" * 60)
        print("[OK] BigQuery with Optimization: ALL TESTS PASSED")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[FAIL] BigQuery with Optimization: TEST FAILED")
        print(f"   Error: {str(e)}")
        pytest.fail(f"BigQuery with Optimization test failed: {str(e)}")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Phase 3: Advanced Features - Sanity Checks")
    print("=" * 60 + "\n")
    
    # Run tests using pytest when called directly
    # This allows the functions to work both as pytest tests and standalone
    try:
        test_cache_service()
        test_query_optimizer()
        test_specialized_agents()
        test_business_insights()
        test_bigquery_with_optimization()
        
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print("   [PASS] Cache Service")
        print("   [PASS] Query Optimizer")
        print("   [PASS] Specialized Agents")
        print("   [PASS] Business Insights")
        print("   [PASS] BigQuery with Optimization")
        print("\n[OK] All Phase 3 sanity checks passed!")
    except AssertionError as e:
        print(f"\n[FAIL] Test assertion failed: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[FAIL] Test error: {str(e)}")
        sys.exit(1)

