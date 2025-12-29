"""Unit tests for business insights prompts"""
import pytest
import pandas as pd
from prompts.business_insights import (
    get_insight_template_for_type,
    format_insights_for_display,
    get_actionable_recommendations_template,
    build_comprehensive_insight_prompt
)


@pytest.mark.unit
class TestBusinessInsights:
    """Test suite for business insights"""
    
    def test_get_insight_template_count(self):
        """Test insight template for count queries"""
        template = get_insight_template_for_type("count")
        
        assert template["focus"] == "scale and magnitude"
        assert len(template["questions"]) > 0
        assert template["format"] == "bullet_points"
    
    def test_get_insight_template_ranking(self):
        """Test insight template for ranking queries"""
        template = get_insight_template_for_type("ranking")
        
        assert template["focus"] == "top performers and patterns"
        assert "top performers" in template["questions"][0].lower()
    
    def test_get_insight_template_aggregation(self):
        """Test insight template for aggregation queries"""
        template = get_insight_template_for_type("aggregation")
        
        assert template["focus"] == "averages and aggregates in business context"
        assert template["format"] == "paragraph"
    
    def test_get_insight_template_temporal(self):
        """Test insight template for temporal queries"""
        template = get_insight_template_for_type("temporal")
        
        assert template["focus"] == "trends over time"
        assert "trend" in template["questions"][0].lower()
    
    def test_get_insight_template_customer_analysis(self):
        """Test insight template for customer analysis"""
        template = get_insight_template_for_type("customer_analysis")
        
        assert template["focus"] == "customer behavior and segmentation"
        assert template["format"] == "segmentation"
    
    def test_format_insights_with_dataframe(self):
        """Test formatting insights with DataFrame"""
        df = pd.DataFrame({
            'revenue': [100, 200, 300],
            'orders': [10, 20, 30]
        })
        
        insights = "Test insights"
        formatted = format_insights_for_display(insights, query_result=df)
        
        assert "Summary Statistics" in formatted
        assert "Rows: 3" in formatted
        assert "Columns: 2" in formatted
    
    def test_format_insights_with_numeric_columns(self):
        """Test formatting insights with numeric columns"""
        df = pd.DataFrame({
            'price': [10.5, 20.0, 30.75],
            'quantity': [1, 2, 3]
        })
        
        insights = "Test insights"
        formatted = format_insights_for_display(insights, query_result=df)
        
        assert "Key Metrics" in formatted
        assert "price" in formatted.lower()
    
    def test_get_actionable_recommendations_customer(self):
        """Test actionable recommendations for customer analysis"""
        recommendations = get_actionable_recommendations_template("customer_analysis")
        
        assert "Recommended Actions" in recommendations
        assert "Segment" in recommendations or "Customer" in recommendations
    
    def test_get_actionable_recommendations_product(self):
        """Test actionable recommendations for product analysis"""
        recommendations = get_actionable_recommendations_template("product_analysis")
        
        assert "Recommended Actions" in recommendations
        assert "Product" in recommendations
    
    def test_build_comprehensive_insight_prompt(self):
        """Test building comprehensive insight prompt"""
        prompt = build_comprehensive_insight_prompt(
            user_query="Show top products",
            query_result_summary="Top 5 products by revenue",
            query_type="ranking",
            include_recommendations=True
        )
        
        assert "Show top products" in prompt
        assert "Top 5 products" in prompt
        assert "actionable recommendations" in prompt.lower()
    
    def test_build_comprehensive_insight_prompt_without_recommendations(self):
        """Test building insight prompt without recommendations"""
        prompt = build_comprehensive_insight_prompt(
            user_query="Count orders",
            query_result_summary="125,451 orders",
            query_type="count",
            include_recommendations=False
        )
        
        assert "Count orders" in prompt
        assert "125,451 orders" in prompt
        assert "actionable recommendations" not in prompt.lower()

