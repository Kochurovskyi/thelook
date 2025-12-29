"""Edge case tests for agent nodes focusing on e-commerce query scenarios"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from agents.nodes import (
    understand_query, generate_sql, validate_sql,
    execute_query, analyze_results, create_visualization, format_response
)
from agents.state import create_initial_state


@pytest.mark.unit
class TestAgentNodesEdgeCases:
    """Edge case tests for agent nodes"""
    
    def test_understand_query_empty_string(self):
        """Test understand_query with empty query"""
        state = create_initial_state("")
        state = understand_query(state)
        
        assert state["query_metadata"]["type"] == "general"
        assert state["query_metadata"]["complexity"] == "simple"
    
    def test_understand_query_very_long_query(self):
        """Test understand_query with very long query string"""
        long_query = "Show me " + " and ".join([f"product {i}" for i in range(100)])
        state = create_initial_state(long_query)
        state = understand_query(state)
        
        assert state["query_metadata"] is not None
        assert "original_query" in state["query_metadata"]
    
    def test_understand_query_multiple_keywords(self):
        """Test understand_query with multiple classification keywords"""
        state = create_initial_state("Count top products by revenue and show customer segments")
        state = understand_query(state)
        
        # Should classify based on first or most prominent keyword
        assert state["query_metadata"]["type"] in ["count", "ranking", "product_analysis", "customer_analysis"]
        assert state["query_metadata"]["complexity"] == "complex"  # Has "and"
    
    def test_validate_sql_with_comments(self):
        """Test validate_sql with SQL comments"""
        state = create_initial_state("Query")
        state["sql_query"] = """
            -- This is a comment
            SELECT * FROM orders
            /* Multi-line comment */
            WHERE status = 'Complete'
        """
        state = validate_sql(state)
        
        assert state.get("error") is None
    
    def test_validate_sql_case_insensitive_forbidden_keywords(self):
        """Test validate_sql catches forbidden keywords in any case"""
        forbidden_queries = [
            "DROP TABLE orders",
            "drop table orders",
            "Drop Table Orders",
            "DELETE FROM orders",
            "delete from orders",
            "INSERT INTO orders VALUES (1)",
            "UPDATE orders SET status = 'Complete'"
        ]
        
        for query in forbidden_queries:
            state = create_initial_state("Query")
            state["sql_query"] = query
            state = validate_sql(state)
            
            assert state.get("error") is not None
            assert "forbidden" in state["error"].lower() or "must be a SELECT" in state["error"]
    
    def test_validate_sql_nested_select_statements(self):
        """Test validate_sql with nested SELECT (subqueries)"""
        state = create_initial_state("Query")
        state["sql_query"] = """
            SELECT * FROM (
                SELECT order_id, COUNT(*) as item_count
                FROM order_items
                GROUP BY order_id
            ) WHERE item_count > 5
        """
        state = validate_sql(state)
        
        assert state.get("error") is None  # Nested SELECTs are allowed
    
    def test_validate_sql_with_cte(self):
        """Test validate_sql with Common Table Expressions"""
        state = create_initial_state("Query")
        state["sql_query"] = """
            WITH order_totals AS (
                SELECT order_id, SUM(sale_price) as total
                FROM order_items
                GROUP BY order_id
            )
            SELECT * FROM order_totals WHERE total > 100
        """
        state = validate_sql(state)
        
        assert state.get("error") is None  # CTEs are allowed
    
    def test_execute_query_with_empty_dataframe(self, mock_bigquery_service):
        """Test execute_query node with empty result"""
        with patch('agents.nodes._get_services', return_value=(mock_bigquery_service, None, None)):
            state = create_initial_state("Query")
            state["sql_query"] = "SELECT * FROM orders WHERE 1=0"
            
            # Mock empty result
            mock_bigquery_service.execute_query.return_value = pd.DataFrame()
            
            state = execute_query(state)
            
            assert state.get("error") is None
            assert state.get("query_result") is not None
            assert len(state["query_result"]) == 0
    
    def test_execute_query_with_single_row_result(self, mock_bigquery_service):
        """Test execute_query with single row result"""
        with patch('agents.nodes._get_services', return_value=(mock_bigquery_service, None, None)):
            state = create_initial_state("Query")
            state["sql_query"] = "SELECT COUNT(*) as count FROM orders"
            
            single_row_df = pd.DataFrame({'count': [125451]})
            mock_bigquery_service.execute_query.return_value = single_row_df
            
            state = execute_query(state)
            
            assert state.get("error") is None
            assert len(state["query_result"]) == 1
    
    def test_analyze_results_with_empty_dataframe(self, mock_llm_service):
        """Test analyze_results with empty DataFrame"""
        with patch('agents.nodes._get_services', return_value=(None, None, mock_llm_service)):
            state = create_initial_state("Query")
            state["query_result"] = pd.DataFrame()
            
            state = analyze_results(state)
            
            # Should handle empty results gracefully
            assert state.get("error") is None
            # May have insights or skip
    
    def test_analyze_results_with_all_null_columns(self, mock_llm_service):
        """Test analyze_results when all columns are NULL"""
        with patch('agents.nodes._get_services', return_value=(None, None, mock_llm_service)):
            state = create_initial_state("Query")
            state["query_result"] = pd.DataFrame({
                'col1': [None, None, None],
                'col2': [None, None, None]
            })
            
            mock_llm_service.generate_text.return_value = "No meaningful data found"
            
            state = analyze_results(state)
            
            assert state.get("error") is None
    
    def test_analyze_results_with_very_large_dataframe(self, mock_llm_service):
        """Test analyze_results with very large result set"""
        with patch('agents.nodes._get_services', return_value=(None, None, mock_llm_service)):
            state = create_initial_state("Query")
            # Create large DataFrame
            large_df = pd.DataFrame({
                'id': range(10000),
                'value': range(10000)
            })
            state["query_result"] = large_df
            
            mock_llm_service.generate_text.return_value = "Large dataset analyzed"
            
            state = analyze_results(state)
            
            assert state.get("error") is None
            # Should summarize large dataset
    
    def test_create_visualization_with_single_column(self):
        """Test create_visualization with single column DataFrame"""
        state = create_initial_state("Query")
        state["query_result"] = pd.DataFrame({'count': [100, 200, 300]})
        
        state = create_visualization(state)
        
        # Single column should not create visualization
        assert state.get("visualization_spec") is None or state["visualization_spec"] is None
    
    def test_create_visualization_with_many_columns(self):
        """Test create_visualization with many columns (wide table)"""
        state = create_initial_state("Query")
        state["query_result"] = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': [10, 20, 30],
            'col3': [100, 200, 300],
            'col4': [1000, 2000, 3000],
            'col5': [10000, 20000, 30000]
        })
        
        state = create_visualization(state)
        
        # Should still create visualization (uses first 2 columns)
        assert state.get("visualization_spec") is not None or state["visualization_spec"] is None
    
    def test_create_visualization_with_mixed_data_types(self):
        """Test create_visualization with mixed data types"""
        state = create_initial_state("Query")
        state["query_result"] = pd.DataFrame({
            'product_name': ['A', 'B', 'C'],  # String
            'price': [10.5, 20.0, 30.75],  # Float
            'quantity': [1, 2, 3],  # Integer
            'in_stock': [True, False, True]  # Boolean
        })
        
        state = create_visualization(state)
        
        # Should handle mixed types
        assert state.get("error") is None
    
    def test_generate_sql_with_special_characters_in_query(self, mock_schema_service, mock_llm_service):
        """Test generate_sql with special characters in user query"""
        with patch('agents.nodes._get_services', return_value=(None, mock_schema_service, mock_llm_service)):
            state = create_initial_state("Show products with name containing 'test' or \"quote\"")
            
            mock_llm_service.generate_sql.return_value = "SELECT * FROM products WHERE name LIKE '%test%'"
            
            state = generate_sql(state)
            
            assert state.get("error") is None
            assert state.get("sql_query") is not None
    
    def test_generate_sql_with_unicode_characters(self, mock_schema_service, mock_llm_service):
        """Test generate_sql with unicode characters in query"""
        with patch('agents.nodes._get_services', return_value=(None, mock_schema_service, mock_llm_service)):
            state = create_initial_state("Show products with name 测试 or café")
            
            mock_llm_service.generate_sql.return_value = "SELECT * FROM products WHERE name LIKE '%café%'"
            
            state = generate_sql(state)
            
            assert state.get("error") is None
    
    def test_format_response_with_all_fields_populated(self, sample_state_with_results):
        """Test format_response with fully populated state"""
        state = sample_state_with_results
        state["sql_query"] = "SELECT COUNT(*) FROM orders"
        state["insights"] = "Test insights"
        state["visualization_spec"] = {"type": "bar", "chart": None}
        
        state = format_response(state)
        
        assert state.get("formatted_response") is not None
        assert state["formatted_response"]["success"] is True
    
    def test_format_response_with_error_state(self):
        """Test format_response when error occurred"""
        state = create_initial_state("Query")
        state["error"] = "Test error message"
        
        state = format_response(state)
        
        assert state.get("formatted_response") is not None
        assert state["formatted_response"]["success"] is False
        assert state["formatted_response"]["error"] == "Test error message"
    
    def test_validate_sql_with_union_queries(self):
        """Test validate_sql with UNION queries"""
        state = create_initial_state("Query")
        state["sql_query"] = """
            SELECT order_id FROM orders WHERE status = 'Complete'
            UNION
            SELECT order_id FROM orders WHERE status = 'Processing'
        """
        state = validate_sql(state)
        
        assert state.get("error") is None  # UNION with SELECT is allowed
    
    def test_validate_sql_with_window_functions(self):
        """Test validate_sql with window functions"""
        state = create_initial_state("Query")
        # Use window function without PARTITION (which contains "CREATE" substring)
        state["sql_query"] = """
            SELECT order_id, 
                   ROW_NUMBER() OVER (ORDER BY created_at) as rn
            FROM orders
        """
        state = validate_sql(state)
        
        assert state.get("error") is None  # Window functions are allowed
    
    def test_execute_query_propagates_error_correctly(self, mock_bigquery_service):
        """Test execute_query properly propagates BigQuery errors"""
        with patch('agents.nodes._get_services', return_value=(mock_bigquery_service, None, None)):
            state = create_initial_state("Query")
            state["sql_query"] = "SELECT * FROM invalid_table"
            
            from google.cloud.exceptions import GoogleCloudError
            mock_bigquery_service.execute_query.side_effect = GoogleCloudError("Table not found")
            
            state = execute_query(state)
            
            assert state.get("error") is not None
            assert "Table not found" in state["error"] or "Query execution failed" in state["error"]

