"""Unit tests for QueryOptimizer"""
import pytest
from utils.query_optimizer import QueryOptimizer


@pytest.mark.unit
class TestQueryOptimizer:
    """Test suite for QueryOptimizer"""
    
    def test_estimate_query_cost_simple(self):
        """Test cost estimation for simple query"""
        query = "SELECT COUNT(*) FROM `bigquery-public-data.thelook_ecommerce.orders`"
        
        cost_info = QueryOptimizer.estimate_query_cost(query)
        
        assert "estimated_bytes_scanned" in cost_info
        assert "estimated_cost_usd" in cost_info
        assert "tables_accessed" in cost_info
        assert "complexity" in cost_info
        # COUNT() is an aggregation, so complexity might be medium
        assert cost_info["complexity"] in ["low", "medium"]
    
    def test_estimate_query_cost_with_joins(self):
        """Test cost estimation for query with JOINs"""
        query = """
        SELECT p.name, SUM(oi.sale_price) as revenue
        FROM `bigquery-public-data.thelook_ecommerce.products` p
        JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON p.id = oi.product_id
        GROUP BY p.name
        """
        
        cost_info = QueryOptimizer.estimate_query_cost(query)
        
        assert cost_info["has_joins"] is True
        assert cost_info["complexity"] in ["medium", "high"]
        assert len(cost_info["tables_accessed"]) >= 2
    
    def test_estimate_query_cost_with_aggregations(self):
        """Test cost estimation for query with aggregations"""
        query = """
        SELECT user_id, COUNT(*) as order_count, SUM(total) as total_spent
        FROM `bigquery-public-data.thelook_ecommerce.orders`
        GROUP BY user_id
        """
        
        cost_info = QueryOptimizer.estimate_query_cost(query)
        
        assert cost_info["has_aggregations"] is True
        assert cost_info["complexity"] in ["medium", "high"]
    
    def test_estimate_query_cost_with_window_functions(self):
        """Test cost estimation for query with window functions"""
        query = """
        SELECT order_id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) as rn
        FROM `bigquery-public-data.thelook_ecommerce.orders`
        """
        
        cost_info = QueryOptimizer.estimate_query_cost(query)
        
        assert cost_info["has_window_functions"] is True
        assert cost_info["complexity"] == "high"
    
    def test_suggest_optimizations_missing_limit(self):
        """Test optimization suggestions for missing LIMIT"""
        query = "SELECT * FROM `bigquery-public-data.thelook_ecommerce.orders`"
        
        suggestions = QueryOptimizer.suggest_optimizations(query)
        
        assert len(suggestions) > 0
        assert any("LIMIT" in s for s in suggestions)
    
    def test_suggest_optimizations_select_star(self):
        """Test optimization suggestions for SELECT *"""
        query = "SELECT * FROM `bigquery-public-data.thelook_ecommerce.orders`"
        
        suggestions = QueryOptimizer.suggest_optimizations(query)
        
        assert any("SELECT *" in s or "specific columns" in s for s in suggestions)
    
    def test_suggest_optimizations_multiple_joins(self):
        """Test optimization suggestions for multiple JOINs"""
        query = """
        SELECT * FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        JOIN users u ON o.user_id = u.id
        JOIN order_items oi2 ON o.order_id = oi2.order_id
        """
        
        suggestions = QueryOptimizer.suggest_optimizations(query)
        
        assert any("JOIN" in s for s in suggestions)
    
    def test_suggest_optimizations_order_by_without_limit(self):
        """Test optimization suggestions for ORDER BY without LIMIT"""
        query = """
        SELECT * FROM `bigquery-public-data.thelook_ecommerce.orders`
        ORDER BY created_at DESC
        """
        
        suggestions = QueryOptimizer.suggest_optimizations(query)
        
        assert any("ORDER BY" in s and "LIMIT" in s for s in suggestions)
    
    def test_validate_query_structure_valid(self):
        """Test query structure validation for valid query"""
        query = "SELECT COUNT(*) FROM `bigquery-public-data.thelook_ecommerce.orders`"
        
        validation = QueryOptimizer.validate_query_structure(query)
        
        assert validation["is_valid"] is True
        assert len(validation["errors"]) == 0
    
    def test_validate_query_structure_forbidden_operation(self):
        """Test query structure validation for forbidden operations"""
        query = "DROP TABLE orders"
        
        validation = QueryOptimizer.validate_query_structure(query)
        
        assert validation["is_valid"] is False
        assert len(validation["errors"]) > 0
        assert any("DROP" in e for e in validation["errors"])
    
    def test_validate_query_structure_no_select(self):
        """Test query structure validation for non-SELECT query"""
        query = "INSERT INTO orders VALUES (1, 2, 'Complete')"
        
        validation = QueryOptimizer.validate_query_structure(query)
        
        assert validation["is_valid"] is False
        assert any("SELECT" in e for e in validation["errors"])
    
    def test_validate_query_structure_warning_missing_dataset(self):
        """Test query structure validation warning for missing dataset"""
        query = "SELECT * FROM orders"
        
        validation = QueryOptimizer.validate_query_structure(query)
        
        # Should have warning but still be valid
        assert validation["is_valid"] is True
        assert len(validation["warnings"]) > 0

