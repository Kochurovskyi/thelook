"""Formatting utilities for results and responses"""
from typing import Dict, Any, Optional
import pandas as pd
from agents.state import AgentState


def format_query_result(df: pd.DataFrame, max_rows: int = 100) -> str:
    """
    Format query results as a readable string
    
    Args:
        df: DataFrame with results
        max_rows: Maximum number of rows to format
        
    Returns:
        Formatted string representation
    """
    if df.empty:
        return "No results returned."
    
    result = f"Query returned {len(df)} rows with {len(df.columns)} columns.\n\n"
    result += "Columns: " + ", ".join(df.columns.tolist()) + "\n\n"
    
    if len(df) <= max_rows:
        result += "Results:\n"
        result += df.to_string(index=False)
    else:
        result += f"First {max_rows} rows:\n"
        result += df.head(max_rows).to_string(index=False)
        result += f"\n\n... and {len(df) - max_rows} more rows"
    
    return result


def format_sql_query(sql: str) -> str:
    """
    Format SQL query for display
    
    Args:
        sql: SQL query string
        
    Returns:
        Formatted SQL string
    """
    # Basic SQL formatting (can be enhanced)
    lines = sql.split('\n')
    formatted_lines = []
    indent = 0
    
    for line in lines:
        stripped = line.strip()
        if not stripped:
            formatted_lines.append('')
            continue
        
        # Decrease indent for closing keywords
        if stripped.upper().startswith((')', 'END', 'ELSE')):
            indent = max(0, indent - 1)
        
        formatted_lines.append('  ' * indent + stripped)
        
        # Increase indent for opening keywords
        if stripped.upper().endswith(('SELECT', 'FROM', 'WHERE', 'JOIN', 'GROUP BY', 'ORDER BY', 'HAVING')):
            indent += 1
    
    return '\n'.join(formatted_lines)


def format_agent_response(state: AgentState) -> Dict[str, Any]:
    """
    Format complete agent response for display
    
    Args:
        state: Final agent state
        
    Returns:
        Dictionary with formatted response components
    """
    response = {
        "query": state.get("query", ""),
        "success": state.get("error") is None,
        "error": state.get("error"),
        "sql_query": None,
        "results": None,
        "insights": state.get("insights"),
        "visualization": None,
        "metadata": state.get("query_metadata")
    }
    
    # Format SQL
    if state.get("sql_query"):
        response["sql_query"] = format_sql_query(state["sql_query"])
    
    # Format results
    if state.get("query_result") is not None:
        df = state["query_result"]
        response["results"] = {
            "dataframe": df,
            "summary": format_query_result(df),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": df.columns.tolist()
        }
    
    # Format visualization
    if state.get("visualization_spec"):
        viz_spec = state["visualization_spec"]
        response["visualization"] = {
            "type": viz_spec.get("type"),
            "chart": viz_spec.get("chart"),
            "x_column": viz_spec.get("x_column"),
            "y_column": viz_spec.get("y_column")
        }
    
    return response


def format_error_message(error: str, context: Optional[str] = None) -> str:
    """
    Format error message for user display
    
    Args:
        error: Error message
        context: Optional context about where error occurred
        
    Returns:
        Formatted error message
    """
    if context:
        return f"Error in {context}: {error}"
    return f"Error: {error}"


def format_insights_for_display(insights: str) -> str:
    """
    Format insights text for better display
    
    Args:
        insights: Raw insights text
        
    Returns:
        Formatted insights
    """
    # Basic formatting - can be enhanced with markdown parsing
    # Split by common patterns and format
    lines = insights.split('\n')
    formatted = []
    
    for line in lines:
        stripped = line.strip()
        if not stripped:
            formatted.append('')
            continue
        
        # Format bullet points
        if stripped.startswith('-') or stripped.startswith('*'):
            formatted.append(stripped)
        # Format numbered lists
        elif stripped[0].isdigit() and stripped[1:3] in ['. ', ') ']:
            formatted.append(stripped)
        # Format headings (lines that are all caps or end with colon)
        elif stripped.isupper() or stripped.endswith(':'):
            formatted.append(f"\n**{stripped}**\n")
        else:
            formatted.append(stripped)
    
    return '\n'.join(formatted)

