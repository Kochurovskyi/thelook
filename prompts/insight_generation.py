"""Insight generation prompts for business analysis"""
from typing import Optional


def get_insight_generation_prompt(
    user_query: str,
    query_result_summary: str,
    query_type: Optional[str] = None
) -> str:
    """
    Build insight generation prompt
    
    Args:
        user_query: Original user question
        query_result_summary: Summary of query results
        query_type: Optional query type (count, ranking, aggregation, etc.)
        
    Returns:
        Complete prompt for insight generation
    """
    base_prompt = f"""You are a business analyst. Analyze the following query results and provide actionable business insights.

USER'S QUESTION: {user_query}

QUERY RESULTS:
{query_result_summary}

INSTRUCTIONS:
- Provide 2-3 key business insights based on this data
- Be specific and use numbers from the results
- Make insights actionable (what should the business do?)
- If the results show trends, explain what they mean
- Highlight any surprising or important findings

FORMAT:
- Use bullet points for clarity
- Start each insight with a bold heading
- Include specific numbers and percentages where relevant
"""
    
    # Add type-specific guidance
    if query_type == "count":
        base_prompt += "\n- Focus on the scale and magnitude of the count\n- Compare to expectations if relevant"
    elif query_type == "ranking":
        base_prompt += "\n- Highlight top performers and their characteristics\n- Identify patterns in the ranking"
    elif query_type == "aggregation":
        base_prompt += "\n- Explain what the average/aggregate means in business context\n- Compare to benchmarks if possible"
    elif query_type == "temporal":
        base_prompt += "\n- Identify trends over time\n- Highlight growth, decline, or seasonal patterns"
    elif query_type == "customer_analysis":
        base_prompt += "\n- Focus on customer behavior and segmentation\n- Provide recommendations for customer engagement"
    elif query_type == "product_analysis":
        base_prompt += "\n- Identify product performance patterns\n- Suggest product strategy recommendations"
    elif query_type == "sales_analysis":
        base_prompt += "\n- Analyze revenue patterns\n- Provide sales strategy recommendations"
    
    return base_prompt


def get_simple_insight_template(query_type: Optional[str] = None) -> str:
    """
    Get a simple insight template for non-LLM fallback
    
    Args:
        query_type: Optional query type
        
    Returns:
        Template string
    """
    templates = {
        "count": "The query returned {count} records. This represents the total number of {entity} in the dataset.",
        "ranking": "The top {n} items are shown. The leading item has {value} {metric}.",
        "aggregation": "The average value is {avg}. This indicates {interpretation}.",
        "default": "Query returned {rows} rows with {cols} columns. Review the data for specific insights."
    }
    
    return templates.get(query_type, templates["default"])

