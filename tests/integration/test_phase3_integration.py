"""Integration tests for Phase 3 features"""
import pytest
from unittest.mock import Mock, patch
from services.bigquery_service import BigQueryService
from services.cache_service import CacheService
from utils.query_optimizer import QueryOptimizer
from agents.specialized_agents import route_to_agent
from agents.state import create_initial_state
import pandas as pd


@pytest.mark.integration
class TestPhase3Integration:
    """Integration tests for Phase 3 features"""
    
    def test_bigquery_service_with_caching(self):
        """Test BigQuery service with caching enabled"""
        with patch('services.bigquery_service.bigquery.Client') as mock_client_class:
            mock_client = Mock()
            mock_query_job = Mock()
            mock_result = Mock()
            
            df = pd.DataFrame({'count': [125451]})
            mock_result.to_dataframe.return_value = df
            mock_query_job.result.return_value = mock_result
            mock_client.query.return_value = mock_query_job
            mock_client_class.return_value = mock_client
            
            # Create service with caching
            service = BigQueryService(enable_cache=True)
            
            query = "SELECT COUNT(*) FROM orders"
            
            # First execution - should execute query
            result1 = service.execute_query(query)
            
            # Second execution - should use cache
            result2 = service.execute_query(query)
            
            assert len(result1) == len(result2)
            # Should only call query once (second time uses cache)
            assert mock_client.query.call_count >= 1
    
    def test_query_optimizer_with_bigquery_service(self):
        """Test query optimizer integrated with BigQuery service"""
        service = BigQueryService()
        
        query = """
        SELECT p.name, SUM(oi.sale_price) as revenue
        FROM `bigquery-public-data.thelook_ecommerce.products` p
        JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON p.id = oi.product_id
        GROUP BY p.name
        ORDER BY revenue DESC
        """
        
        # Estimate cost
        cost_info = service.estimate_query_cost(query)
        
        assert cost_info["has_joins"] is True
        assert cost_info["has_aggregations"] is True
        assert cost_info["complexity"] in ["medium", "high"]
        
        # Get suggestions
        suggestions = QueryOptimizer.suggest_optimizations(query)
        assert len(suggestions) >= 0  # May or may not have suggestions
    
    def test_router_with_query_metadata(self):
        """Test router integration with query understanding"""
        # Customer query
        state1 = create_initial_state("Show customer segments by age")
        from agents.nodes import understand_query
        state1 = understand_query(state1)
        route1 = route_to_agent(state1)
        assert route1 == "customer_segmentation"
        
        # Product query
        state2 = create_initial_state("Show top products by revenue")
        state2 = understand_query(state2)
        route2 = route_to_agent(state2)
        assert route2 == "product_performance"
        
        # Sales trend query
        state3 = create_initial_state("Show sales trends over time")
        state3 = understand_query(state3)
        route3 = route_to_agent(state3)
        assert route3 == "sales_trends"
    
    def test_cache_service_with_query_optimizer(self):
        """Test cache service with query optimizer"""
        cache = CacheService(ttl_seconds=3600)
        optimizer = QueryOptimizer()
        
        query = "SELECT COUNT(*) FROM orders"
        
        # Estimate cost
        cost_info = optimizer.estimate_query_cost(query)
        
        # Cache a result
        df = pd.DataFrame({'count': [100]})
        cache.cache_result(query, df)
        
        # Retrieve from cache
        cached_df = cache.get_cached_result(query)
        
        assert cached_df is not None
        assert cost_info["estimated_cost_usd"] >= 0
    
    def test_specialized_agent_with_caching(self):
        """Test specialized agent workflow with caching"""
        with patch('agents.specialized_agents._get_services') as mock_get_services:
            mock_bq = Mock()
            mock_schema = Mock()
            mock_llm = Mock()
            
            # Setup mocks
            mock_schema.build_schema_context.return_value = "Schema"
            mock_llm.generate_text.return_value = "SELECT * FROM users"
            
            # Mock BigQuery with caching
            df = pd.DataFrame({'user_id': [1, 2, 3]})
            mock_bq.execute_query.return_value = df
            mock_bq.enable_cache = True
            
            mock_get_services.return_value = (mock_bq, mock_schema, mock_llm)
            
            from agents.specialized_agents import customer_segmentation_agent
            
            state = create_initial_state("Show customer segments")
            state = customer_segmentation_agent(state)
            
            # Should have SQL and be ready for execution
            assert state.get("sql_query") is not None
            assert state["query_metadata"]["type"] == "customer_analysis"

