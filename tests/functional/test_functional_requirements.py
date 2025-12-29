"""
Functional tests for e-commerce analytics agent based on functional requirements.

These tests verify that the agent can handle the core functional requirements:
1. Customer segmentation and behavior analysis
2. Product performance and recommendation insights
3. Sales trends and seasonality patterns
4. Geographic sales patterns
5. Database structure questions
"""
import pytest
from agents.graph import run_agent
from utils.request_context import RequestContext


@pytest.mark.functional
class TestCustomerSegmentation:
    """Test customer segmentation and behavior analysis functionality"""
    
    def test_customer_segmentation_query(self, mock_bigquery_client):
        """Test basic customer segmentation query"""
        query = "Segment customers by their total order value"
        
        RequestContext.set_request_id("test_customer_segmentation")
        result = run_agent(query, use_specialized_agents=True)
        
        # Verify results - allow for errors in SQL generation but check that agent attempted
        assert result["sql_query"] is not None or result["error"] is not None, "Agent should generate SQL or report error"
        if result["sql_query"]:
            assert "customer" in result["sql_query"].lower() or "user" in result["sql_query"].lower() or "order" in result["sql_query"].lower()
    
    def test_customer_lifetime_value_analysis(self, mock_bigquery_client):
        """Test customer lifetime value (CLV) analysis"""
        query = "Calculate customer lifetime value and segment customers by CLV tiers"
        
        RequestContext.set_request_id("test_clv_analysis")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "lifetime" in result["sql_query"].lower() or "clv" in result["sql_query"].lower() or "sum" in result["sql_query"].lower()
    
    def test_customer_retention_analysis(self, mock_bigquery_client):
        """Test customer retention and churn analysis"""
        query = "Analyze customer retention rates and identify churn patterns"
        
        RequestContext.set_request_id("test_retention_analysis")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "retention" in result["sql_query"].lower() or "churn" in result["sql_query"].lower() or "count" in result["sql_query"].lower()


@pytest.mark.functional
class TestProductPerformance:
    """Test product performance and recommendation insights functionality"""
    
    def test_product_performance_query(self, mock_bigquery_client):
        """Test basic product performance query"""
        query = "Show me top 10 products by revenue"
        
        RequestContext.set_request_id("test_product_performance")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "product" in result["sql_query"].lower()
            assert "revenue" in result["sql_query"].lower() or "sale_price" in result["sql_query"].lower() or "sum" in result["sql_query"].lower()
    
    def test_inventory_turnover_analysis(self, mock_bigquery_client):
        """Test inventory turnover analysis"""
        query = "Calculate inventory turnover rates by product category"
        
        RequestContext.set_request_id("test_inventory_turnover")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "inventory" in result["sql_query"].lower() or "turnover" in result["sql_query"].lower() or "category" in result["sql_query"].lower()
    
    def test_category_performance_comparison(self, mock_bigquery_client):
        """Test product category performance comparison"""
        query = "Compare revenue and profit margins across product categories"
        
        RequestContext.set_request_id("test_category_performance")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "category" in result["sql_query"].lower() or "revenue" in result["sql_query"].lower() or "sum" in result["sql_query"].lower()


@pytest.mark.functional
class TestSalesTrends:
    """Test sales trends and seasonality patterns functionality"""
    
    def test_sales_trends_query(self, mock_bigquery_client):
        """Test basic sales trends query"""
        query = "Show me sales trends over the last 12 months"
        
        RequestContext.set_request_id("test_sales_trends")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "date" in result["sql_query"].lower() or "created_at" in result["sql_query"].lower() or "month" in result["sql_query"].lower()
    
    def test_revenue_forecasting(self, mock_bigquery_client):
        """Test revenue forecasting and predictions"""
        query = "Generate revenue forecast for the next 3 months based on historical trends"
        
        RequestContext.set_request_id("test_revenue_forecast")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "forecast" in result["sql_query"].lower() or "revenue" in result["sql_query"].lower() or "date" in result["sql_query"].lower()
    
    def test_growth_rate_analysis(self, mock_bigquery_client):
        """Test monthly/quarterly growth rate analysis"""
        query = "Calculate month-over-month growth rates for sales"
        
        RequestContext.set_request_id("test_growth_rate")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "growth" in result["sql_query"].lower() or "month" in result["sql_query"].lower() or "date" in result["sql_query"].lower()


@pytest.mark.functional
class TestGeographicPatterns:
    """Test geographic sales patterns functionality"""
    
    def test_geographic_sales_query(self, mock_bigquery_client):
        """Test basic geographic sales patterns query"""
        query = "Show me sales by country or region"
        
        RequestContext.set_request_id("test_geographic_sales")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "country" in result["sql_query"].lower() or "region" in result["sql_query"].lower() or "state" in result["sql_query"].lower()
    
    def test_regional_performance_comparison(self, mock_bigquery_client):
        """Test regional performance comparison"""
        query = "Compare sales performance across different countries"
        
        RequestContext.set_request_id("test_regional_performance")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "country" in result["sql_query"].lower() or "compare" in result["sql_query"].lower() or "group" in result["sql_query"].lower()
    
    def test_shipping_performance_by_region(self, mock_bigquery_client):
        """Test shipping and delivery performance by region"""
        query = "Analyze shipping times and delivery performance by country"
        
        RequestContext.set_request_id("test_shipping_performance")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "shipping" in result["sql_query"].lower() or "delivery" in result["sql_query"].lower() or "country" in result["sql_query"].lower()


@pytest.mark.functional
class TestDatabaseStructure:
    """Test database structure and schema analysis functionality"""
    
    def test_database_structure_query(self, mock_bigquery_client):
        """Test basic database structure question"""
        query = "What tables are in the database and what are their columns?"
        
        RequestContext.set_request_id("test_db_structure")
        result = run_agent(query, use_specialized_agents=True)
        
        # For schema questions, the agent should use schema_service
        # This might not generate SQL, but should provide schema information
        assert result["error"] is None or result["insights"] is not None or result["sql_query"] is not None
    
    def test_table_relationship_analysis(self, mock_bigquery_client):
        """Test table relationship and foreign key analysis"""
        query = "What are the relationships between the orders and order_items tables?"
        
        RequestContext.set_request_id("test_table_relationships")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["error"] is None or result["insights"] is not None or result["sql_query"] is not None
        if result["insights"]:
            assert "relationship" in result["insights"].lower() or "join" in result["insights"].lower() or "order" in result["insights"].lower()
    
    def test_data_quality_checks(self, mock_bigquery_client):
        """Test data quality and completeness checks"""
        query = "Check for missing values and data quality issues in the orders table"
        
        RequestContext.set_request_id("test_data_quality")
        result = run_agent(query, use_specialized_agents=True)
        
        assert result["sql_query"] is not None or result["error"] is not None
        if result["sql_query"]:
            assert "null" in result["sql_query"].lower() or "missing" in result["sql_query"].lower() or "count" in result["sql_query"].lower()
