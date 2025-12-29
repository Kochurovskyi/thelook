"""Unit tests for specialized agents"""
import pytest
from unittest.mock import Mock, patch
from agents.specialized_agents import (
    route_to_agent, customer_segmentation_agent,
    product_performance_agent, sales_trends_agent,
    geographic_analysis_agent
)
from agents.state import create_initial_state


@pytest.mark.unit
class TestSpecializedAgents:
    """Test suite for specialized agents"""
    
    def test_route_to_agent_customer(self):
        """Test router for customer-related queries"""
        state = create_initial_state("Show customer segments by age")
        route = route_to_agent(state)
        
        assert route == "customer_segmentation"
    
    def test_route_to_agent_product(self):
        """Test router for product-related queries"""
        state = create_initial_state("Show top products by revenue")
        route = route_to_agent(state)
        
        assert route == "product_performance"
    
    def test_route_to_agent_sales_trends(self):
        """Test router for sales/trend queries"""
        state = create_initial_state("Show sales trends over time")
        route = route_to_agent(state)
        
        assert route == "sales_trends"
    
    def test_route_to_agent_geographic(self):
        """Test router for geographic queries"""
        state = create_initial_state("Show sales by location")
        route = route_to_agent(state)
        
        assert route == "geographic"
    
    def test_route_to_agent_general(self):
        """Test router for general queries"""
        state = create_initial_state("Count orders")
        route = route_to_agent(state)
        
        assert route == "general"
    
    def test_route_to_agent_with_metadata(self):
        """Test router with query metadata"""
        state = create_initial_state("Query")
        state["query_metadata"] = {"type": "customer_analysis"}
        route = route_to_agent(state)
        
        assert route == "customer_segmentation"
    
    @patch('agents.specialized_agents._get_services')
    def test_customer_segmentation_agent(self, mock_get_services):
        """Test customer segmentation agent"""
        # Mock services
        mock_bq = Mock()
        mock_schema = Mock()
        mock_llm = Mock()
        mock_schema.build_schema_context.return_value = "Schema context"
        mock_llm.generate_text.return_value = "SELECT * FROM users"
        
        mock_get_services.return_value = (mock_bq, mock_schema, mock_llm)
        
        state = create_initial_state("Show customer segments")
        state = customer_segmentation_agent(state)
        
        assert state["query_metadata"]["type"] == "customer_analysis"
        assert state.get("sql_query") is not None
    
    @patch('agents.specialized_agents._get_services')
    def test_product_performance_agent(self, mock_get_services):
        """Test product performance agent"""
        # Mock services
        mock_bq = Mock()
        mock_schema = Mock()
        mock_llm = Mock()
        mock_schema.build_schema_context.return_value = "Schema context"
        mock_llm.generate_text.return_value = "SELECT * FROM products"
        
        mock_get_services.return_value = (mock_bq, mock_schema, mock_llm)
        
        state = create_initial_state("Show top products")
        state = product_performance_agent(state)
        
        assert state["query_metadata"]["type"] == "product_analysis"
        assert state.get("sql_query") is not None
    
    @patch('agents.specialized_agents._get_services')
    def test_sales_trends_agent(self, mock_get_services):
        """Test sales trends agent"""
        # Mock services
        mock_bq = Mock()
        mock_schema = Mock()
        mock_llm = Mock()
        mock_schema.build_schema_context.return_value = "Schema context"
        mock_llm.generate_text.return_value = "SELECT * FROM orders"
        
        mock_get_services.return_value = (mock_bq, mock_schema, mock_llm)
        
        state = create_initial_state("Show sales over time")
        state = sales_trends_agent(state)
        
        assert state["query_metadata"]["type"] == "temporal"
        assert state.get("sql_query") is not None
    
    @patch('agents.specialized_agents._get_services')
    def test_geographic_analysis_agent(self, mock_get_services):
        """Test geographic analysis agent"""
        # Mock services
        mock_bq = Mock()
        mock_schema = Mock()
        mock_llm = Mock()
        mock_schema.build_schema_context.return_value = "Schema context"
        mock_llm.generate_text.return_value = "SELECT state, COUNT(*) FROM orders GROUP BY state"
        
        mock_get_services.return_value = (mock_bq, mock_schema, mock_llm)
        
        state = create_initial_state("Show sales by location")
        state = geographic_analysis_agent(state)
        
        assert state["query_metadata"]["type"] == "geographic"
        assert state.get("sql_query") is not None

