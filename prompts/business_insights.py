"""Business insight templates and prompts for different analysis types"""
from typing import Optional, Dict, Any
import pandas as pd


def get_insight_template_for_type(query_type: str) -> Dict[str, Any]:
    """
    Get insight generation template based on query type
    
    Args:
        query_type: Type of query (count, ranking, aggregation, etc.)
        
    Returns:
        Dictionary with template configuration
    """
    templates = {
        "count": {
            "focus": "scale and magnitude",
            "questions": [
                "What does this count represent in business terms?",
                "Is this number higher or lower than expected?",
                "What actions should be taken based on this count?"
            ],
            "format": "bullet_points"
        },
        "ranking": {
            "focus": "top performers and patterns",
            "questions": [
                "Who/what are the top performers?",
                "What characteristics do top performers share?",
                "What can we learn from the ranking?",
                "Are there any surprises in the ranking?"
            ],
            "format": "numbered_list"
        },
        "aggregation": {
            "focus": "averages and aggregates in business context",
            "questions": [
                "What does this average/aggregate mean?",
                "How does this compare to industry benchmarks?",
                "Is this value healthy for the business?",
                "What trends does this indicate?"
            ],
            "format": "paragraph"
        },
        "temporal": {
            "focus": "trends over time",
            "questions": [
                "What is the trend direction (up, down, stable)?",
                "Are there seasonal patterns?",
                "What caused any significant changes?",
                "What should the business do based on this trend?"
            ],
            "format": "timeline"
        },
        "customer_analysis": {
            "focus": "customer behavior and segmentation",
            "questions": [
                "What customer segments exist?",
                "How do different segments behave?",
                "What are the characteristics of high-value customers?",
                "How can we better engage each segment?"
            ],
            "format": "segmentation"
        },
        "product_analysis": {
            "focus": "product performance patterns",
            "questions": [
                "Which products perform best?",
                "What makes successful products different?",
                "Are there underperforming products?",
                "What product strategy changes are needed?"
            ],
            "format": "comparison"
        },
        "sales_analysis": {
            "focus": "revenue patterns and sales strategy",
            "questions": [
                "What are the revenue trends?",
                "Which channels/products drive most revenue?",
                "What factors influence sales performance?",
                "What sales strategy changes are recommended?"
            ],
            "format": "strategic"
        }
    }
    
    return templates.get(query_type, templates["count"])


def format_insights_for_display(
    insights: str,
    query_type: Optional[str] = None,
    query_result: Optional[pd.DataFrame] = None
) -> str:
    """
    Format insights for better display in UI
    
    Args:
        insights: Raw insight text
        query_type: Optional query type
        query_result: Optional query results DataFrame
        
    Returns:
        Formatted insight string
    """
    # Add summary statistics if available
    if query_result is not None and not query_result.empty:
        summary = f"\n**Summary Statistics:**\n"
        summary += f"- Rows: {len(query_result)}\n"
        summary += f"- Columns: {len(query_result.columns)}\n"
        
        # Add numeric column summaries
        numeric_cols = query_result.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            summary += f"\n**Key Metrics:**\n"
            for col in numeric_cols[:3]:  # Limit to first 3 numeric columns
                summary += f"- {col}: min={query_result[col].min():.2f}, max={query_result[col].max():.2f}, avg={query_result[col].mean():.2f}\n"
        
        insights = summary + "\n" + insights
    
    return insights


def get_actionable_recommendations_template(query_type: str) -> str:
    """
    Get template for actionable recommendations based on query type
    
    Args:
        query_type: Type of query
        
    Returns:
        Template string with recommendation structure
    """
    templates = {
        "customer_analysis": """
**Recommended Actions:**
1. **Segment-Specific Marketing**: Develop targeted campaigns for each customer segment
2. **Retention Strategy**: Focus on high-value customer retention
3. **Acquisition**: Identify characteristics of valuable customers for targeting
""",
        "product_analysis": """
**Recommended Actions:**
1. **Product Optimization**: Improve underperforming products or consider discontinuation
2. **Inventory Management**: Adjust stock levels based on performance
3. **Pricing Strategy**: Review pricing for top and bottom performers
""",
        "sales_analysis": """
**Recommended Actions:**
1. **Channel Optimization**: Invest more in high-performing sales channels
2. **Seasonal Planning**: Adjust inventory and marketing for seasonal patterns
3. **Revenue Growth**: Focus on strategies that drive revenue increases
""",
        "temporal": """
**Recommended Actions:**
1. **Trend Response**: Adjust strategy based on identified trends
2. **Seasonal Planning**: Prepare for expected seasonal changes
3. **Anomaly Investigation**: Investigate and address any unexpected changes
"""
    }
    
    return templates.get(query_type, """
**Recommended Actions:**
1. Review the data insights carefully
2. Consider business context when interpreting results
3. Take action based on key findings
""")


def build_comprehensive_insight_prompt(
    user_query: str,
    query_result_summary: str,
    query_type: Optional[str] = None,
    include_recommendations: bool = True
) -> str:
    """
    Build a comprehensive insight generation prompt
    
    Args:
        user_query: Original user question
        query_result_summary: Summary of query results
        query_type: Optional query type
        include_recommendations: Whether to include actionable recommendations
        
    Returns:
        Complete prompt for insight generation
    """
    from prompts.insight_generation import get_insight_generation_prompt
    
    base_prompt = get_insight_generation_prompt(user_query, query_result_summary, query_type)
    
    if query_type:
        template = get_insight_template_for_type(query_type)
        base_prompt += f"\n\nFOCUS AREAS:\n"
        base_prompt += f"- {template['focus']}\n"
        base_prompt += f"\nKEY QUESTIONS TO ANSWER:\n"
        for q in template['questions']:
            base_prompt += f"- {q}\n"
    
    if include_recommendations:
        base_prompt += "\n\nInclude actionable recommendations based on your insights."
    
    return base_prompt

