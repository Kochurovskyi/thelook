"""LangGraph workflow definition for analytics agent"""
from langgraph.graph import StateGraph, END
from agents.state import AgentState
from agents.nodes import (
    understand_query, generate_sql, validate_sql,
    execute_query, analyze_results, create_visualization,
    format_response
)
from agents.specialized_agents import (
    customer_segmentation_agent, product_performance_agent,
    sales_trends_agent, geographic_analysis_agent, route_to_agent
)


def should_continue(state: AgentState) -> str:
    """
    Conditional edge function to determine next step
    
    Args:
        state: Current agent state
        
    Returns:
        Next node name or END
    """
    if state.get("error"):
        return "end"
    
    if state.get("sql_query") is None:
        return "generate_sql"
    
    if state.get("query_result") is None:
        if state.get("sql_query"):
            return "execute_query"
        return "end"
    
    if state.get("insights") is None:
        return "analyze_results"
    
    return "create_visualization"


def create_agent_graph(use_specialized_agents: bool = True) -> StateGraph:
    """
    Create and compile the LangGraph workflow with optional multi-agent support
    
    Args:
        use_specialized_agents: Whether to use specialized agents or general workflow
        
    Returns:
        Compiled StateGraph
    """
    # Create graph
    workflow = StateGraph(AgentState)
    
    if use_specialized_agents:
        # Multi-agent workflow with router
        # Add router node (understand_query)
        workflow.add_node("understand_query", understand_query)
        
        # Add specialized agent nodes
        workflow.add_node("customer_segmentation", customer_segmentation_agent)
        workflow.add_node("product_performance", product_performance_agent)
        workflow.add_node("sales_trends", sales_trends_agent)
        workflow.add_node("geographic", geographic_analysis_agent)
        
        # Add general workflow nodes (fallback)
        workflow.add_node("generate_sql", generate_sql)
        workflow.add_node("validate_sql", validate_sql)
        workflow.add_node("execute_query", execute_query)
        workflow.add_node("analyze_results", analyze_results)
        workflow.add_node("create_visualization", create_visualization)
        workflow.add_node("format_response", format_response)
        
        # Set entry point
        workflow.set_entry_point("understand_query")
        
        # Route from understand_query to appropriate agent
        workflow.add_conditional_edges(
            "understand_query",
            route_to_agent,
            {
                "customer_segmentation": "customer_segmentation",
                "product_performance": "product_performance",
                "sales_trends": "sales_trends",
                "geographic": "geographic",
                "general": "generate_sql"
            }
        )
        
        # All specialized agents end at format_response
        workflow.add_edge("customer_segmentation", "format_response")
        workflow.add_edge("product_performance", "format_response")
        workflow.add_edge("sales_trends", "format_response")
        workflow.add_edge("geographic", "format_response")
        
        # General workflow path
        workflow.add_edge("generate_sql", "validate_sql")
        workflow.add_conditional_edges(
            "validate_sql",
            should_continue,
            {
                "execute_query": "execute_query",
                "end": END
            }
        )
        workflow.add_edge("execute_query", "analyze_results")
        workflow.add_edge("analyze_results", "create_visualization")
        workflow.add_edge("create_visualization", "format_response")
        workflow.add_edge("format_response", END)
        
    else:
        # Original single-agent workflow
        workflow.add_node("understand_query", understand_query)
        workflow.add_node("generate_sql", generate_sql)
        workflow.add_node("validate_sql", validate_sql)
        workflow.add_node("execute_query", execute_query)
        workflow.add_node("analyze_results", analyze_results)
        workflow.add_node("create_visualization", create_visualization)
        workflow.add_node("format_response", format_response)
        
        workflow.set_entry_point("understand_query")
        workflow.add_edge("understand_query", "generate_sql")
        workflow.add_edge("generate_sql", "validate_sql")
        workflow.add_conditional_edges(
            "validate_sql",
            should_continue,
            {
                "execute_query": "execute_query",
                "end": END
            }
        )
        workflow.add_edge("execute_query", "analyze_results")
        workflow.add_edge("analyze_results", "create_visualization")
        workflow.add_edge("create_visualization", "format_response")
        workflow.add_edge("format_response", END)
    
    # Compile graph
    return workflow.compile()


def run_agent(query: str, use_specialized_agents: bool = True) -> AgentState:
    """
    Run the agent workflow with a user query
    
    Args:
        query: User's natural language query
        use_specialized_agents: Whether to use specialized agents
        
    Returns:
        Final agent state with results
    """
    from agents.state import create_initial_state
    
    graph = create_agent_graph(use_specialized_agents=use_specialized_agents)
    initial_state = create_initial_state(query)
    
    # Run graph
    final_state = graph.invoke(initial_state)
    
    return final_state

