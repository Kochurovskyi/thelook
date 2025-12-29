"""Agent state schema for LangGraph workflow"""
from typing import TypedDict, Optional, List, Dict, Any
from langchain_core.messages import BaseMessage
import pandas as pd


class AgentState(TypedDict):
    """State schema for the analytics agent workflow"""
    
    # Messages for conversation history
    messages: List[BaseMessage]
    
    # User query
    query: str
    
    # Generated SQL query
    sql_query: Optional[str]
    
    # Query execution result
    query_result: Optional[pd.DataFrame]
    
    # Business insights generated from results
    insights: Optional[str]
    
    # Visualization specification (Altair chart spec)
    visualization_spec: Optional[Dict[str, Any]]
    
    # Error message if any step fails
    error: Optional[str]
    
    # Query metadata (type, complexity, etc.)
    query_metadata: Optional[Dict[str, Any]]
    
    # Retry tracking
    retry_count: Optional[int]
    
    # Previous errors for recovery
    previous_errors: Optional[List[str]]


def create_initial_state(query: str) -> AgentState:
    """
    Create initial agent state from user query
    
    Args:
        query: User's natural language query
        
    Returns:
        Initial AgentState with query set
    """
    from langchain_core.messages import HumanMessage
    
    return AgentState(
        messages=[HumanMessage(content=query)],
        query=query,
        sql_query=None,
        query_result=None,
        insights=None,
        visualization_spec=None,
        error=None,
        query_metadata=None,
        retry_count=0,
        previous_errors=None
    )

