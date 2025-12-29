"""Specialized agents for different analysis types"""
from typing import Dict, Any
from agents.state import AgentState
from agents.nodes import (
    understand_query, generate_sql, validate_sql,
    execute_query, analyze_results, create_visualization
)
from prompts.sql_generation import build_dynamic_prompt, get_few_shot_examples
from prompts.business_insights import get_insight_template_for_type, build_comprehensive_insight_prompt
from services.schema_service import SchemaService
from services.llm_service import LLMService


def customer_segmentation_agent(state: AgentState) -> AgentState:
    """
    Specialized agent for customer segmentation and behavior analysis
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state with customer-focused analysis
    """
    # Enhance query understanding for customer analysis
    if not state.get("query_metadata"):
        state = understand_query(state)
    
    # Override query type to customer_analysis
    state["query_metadata"]["type"] = "customer_analysis"
    state["query_metadata"]["complexity"] = "medium"
    
    # Use customer-specific few-shot examples
    _, schema_service, llm_service = _get_services()
    
    if llm_service and schema_service:
        schema_context = schema_service.build_schema_context()
        customer_examples = get_few_shot_examples("customer_analysis")
        
        # Generate SQL with customer-focused prompt
        prompt = build_dynamic_prompt(
            state["query"],
            schema_context,
            state.get("query_metadata"),
            state.get("previous_errors")
        )
        
        # Use generate_text since we built the full prompt
        sql = llm_service.generate_text(prompt)
        # Clean up SQL - remove markdown code blocks if present
        sql = sql.strip()
        if sql.startswith("```sql"):
            sql = sql[6:]
        elif sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        state["sql_query"] = sql.strip()
    
    # Continue with standard workflow
    state = validate_sql(state)
    if not state.get("error"):
        state = execute_query(state)
        if not state.get("error"):
            state = analyze_results(state)
            state = create_visualization(state)
    
    return state


def product_performance_agent(state: AgentState) -> AgentState:
    """
    Specialized agent for product performance analysis
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state with product-focused analysis
    """
    # Enhance query understanding for product analysis
    if not state.get("query_metadata"):
        state = understand_query(state)
    
    # Override query type to product_analysis
    state["query_metadata"]["type"] = "product_analysis"
    state["query_metadata"]["complexity"] = "medium"
    
    # Use product-specific few-shot examples
    _, schema_service, llm_service = _get_services()
    
    if llm_service and schema_service:
        schema_context = schema_service.build_schema_context()
        product_examples = get_few_shot_examples("product_analysis")
        
        # Generate SQL with product-focused prompt
        prompt = build_dynamic_prompt(
            state["query"],
            schema_context,
            state.get("query_metadata"),
            state.get("previous_errors")
        )
        
        # Use generate_text since we built the full prompt
        sql = llm_service.generate_text(prompt)
        # Clean up SQL - remove markdown code blocks if present
        sql = sql.strip()
        if sql.startswith("```sql"):
            sql = sql[6:]
        elif sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        state["sql_query"] = sql.strip()
    
    # Continue with standard workflow
    state = validate_sql(state)
    if not state.get("error"):
        state = execute_query(state)
        if not state.get("error"):
            state = analyze_results(state)
            state = create_visualization(state)
    
    return state


def sales_trends_agent(state: AgentState) -> AgentState:
    """
    Specialized agent for sales trends and temporal analysis
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state with sales trend analysis
    """
    # Enhance query understanding for temporal analysis
    if not state.get("query_metadata"):
        state = understand_query(state)
    
    # Override query type to temporal
    state["query_metadata"]["type"] = "temporal"
    state["query_metadata"]["complexity"] = "complex"
    
    # Use temporal-specific few-shot examples
    _, schema_service, llm_service = _get_services()
    
    if llm_service and schema_service:
        schema_context = schema_service.build_schema_context()
        temporal_examples = get_few_shot_examples("temporal")
        
        # Generate SQL with temporal-focused prompt
        prompt = build_dynamic_prompt(
            state["query"],
            schema_context,
            state.get("query_metadata"),
            state.get("previous_errors")
        )
        
        # Use generate_text since we built the full prompt
        sql = llm_service.generate_text(prompt)
        # Clean up SQL - remove markdown code blocks if present
        sql = sql.strip()
        if sql.startswith("```sql"):
            sql = sql[6:]
        elif sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        state["sql_query"] = sql.strip()
    
    # Continue with standard workflow
    state = validate_sql(state)
    if not state.get("error"):
        state = execute_query(state)
        if not state.get("error"):
            state = analyze_results(state)
            state = create_visualization(state)
    
    return state


def geographic_analysis_agent(state: AgentState) -> AgentState:
    """
    Specialized agent for geographic sales patterns
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state with geographic analysis
    """
    # Enhance query understanding for geographic analysis
    if not state.get("query_metadata"):
        state = understand_query(state)
    
    # Override query type
    state["query_metadata"]["type"] = "geographic"
    state["query_metadata"]["complexity"] = "medium"
    
    # Use geographic-focused prompt
    _, schema_service, llm_service = _get_services()
    
    if llm_service and schema_service:
        schema_context = schema_service.build_schema_context()
        
        # Add geographic-specific guidance
        geographic_prompt = f"""{build_dynamic_prompt(
            state["query"],
            schema_context,
            state.get("query_metadata"),
            state.get("previous_errors")
        )}

GEOGRAPHIC ANALYSIS GUIDANCE:
- Use state, country, or region columns from users or orders tables
- Group by geographic location
- Consider using geographic visualizations (maps if supported)
"""
        
        # Use generate_text since we built the full prompt
        sql = llm_service.generate_text(geographic_prompt)
        # Clean up SQL - remove markdown code blocks if present
        sql = sql.strip()
        if sql.startswith("```sql"):
            sql = sql[6:]
        elif sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        state["sql_query"] = sql.strip()
    
    # Continue with standard workflow
    state = validate_sql(state)
    if not state.get("error"):
        state = execute_query(state)
        if not state.get("error"):
            state = analyze_results(state)
            state = create_visualization(state)
    
    return state


def _get_services():
    """Get service instances (shared with nodes.py)"""
    from agents.nodes import _get_services as get_nodes_services
    return get_nodes_services()


def route_to_agent(state: AgentState) -> str:
    """
    Router function to determine which specialized agent to use
    
    Args:
        state: Current agent state
        
    Returns:
        Agent name: 'customer_segmentation', 'product_performance', 'sales_trends', 'geographic', or 'general'
    """
    query = state.get("query", "").lower()
    query_metadata = state.get("query_metadata") or {}
    query_type = query_metadata.get("type", "general") if query_metadata else "general"
    
    # Geographic keywords (check first to avoid conflicts with customer keywords)
    geo_keywords = ["geographic", "by location", "by state", "by country", "by region", "by city", "location", "state", "country", "region", "city"]
    if any(kw in query for kw in geo_keywords):
        return "geographic"
    
    # Customer segmentation keywords (exclude "location" which is handled above)
    customer_keywords = ["customer", "user", "segment", "demographic", "age", "gender"]
    if any(kw in query for kw in customer_keywords) or query_type == "customer_analysis":
        return "customer_segmentation"
    
    # Product performance keywords
    product_keywords = ["product", "item", "catalog", "inventory", "category", "brand"]
    if any(kw in query for kw in product_keywords) or query_type == "product_analysis":
        return "product_performance"
    
    # Sales trends keywords
    temporal_keywords = ["trend", "over time", "by month", "by year", "by day", "growth", "decline", "seasonal"]
    sales_keywords = ["revenue", "sales", "income", "profit"]
    if any(kw in query for kw in temporal_keywords + sales_keywords) or query_type in ["temporal", "sales_analysis"]:
        return "sales_trends"
    
    # Default to general agent
    return "general"

