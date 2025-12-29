"""Agent nodes for LangGraph workflow"""
from typing import Dict, Any
import pandas as pd
import re
from agents.state import AgentState
from services.bigquery_service import BigQueryService
from services.schema_service import SchemaService
from services.llm_service import LLMService
import config


# Initialize services (will be reused across nodes)
_bigquery_service = None
_schema_service = None
_llm_service = None


def _get_services():
    """Get or initialize service instances"""
    global _bigquery_service, _schema_service, _llm_service
    
    if _bigquery_service is None:
        _bigquery_service = BigQueryService()
    if _schema_service is None:
        _schema_service = SchemaService(_bigquery_service)
    if _llm_service is None:
        try:
            _llm_service = LLMService()
        except ValueError:
            # API key not set, will handle in nodes that need it
            pass
    
    return _bigquery_service, _schema_service, _llm_service


def understand_query(state: AgentState) -> AgentState:
    """
    Node 6.1: Understand and classify the user query
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state with query metadata
    """
    query = state["query"].lower()
    
    # Simple rule-based classification (can be enhanced with LLM)
    query_type = "general"
    complexity = "simple"
    
    if any(word in query for word in ["count", "number", "how many"]):
        query_type = "count"
        complexity = "simple"
    elif any(word in query for word in ["top", "best", "highest", "most"]):
        query_type = "ranking"
        complexity = "medium"
    elif any(word in query for word in ["average", "mean", "avg"]):
        query_type = "aggregation"
        complexity = "medium"
    elif any(word in query for word in ["trend", "over time", "by month", "by year"]):
        query_type = "temporal"
        complexity = "complex"
    elif any(word in query for word in ["customer", "user", "segment"]):
        query_type = "customer_analysis"
        complexity = "medium"
    elif any(word in query for word in ["product", "item", "catalog"]):
        query_type = "product_analysis"
        complexity = "medium"
    elif any(word in query for word in ["revenue", "sales", "income"]):
        query_type = "sales_analysis"
        complexity = "medium"
    
    # Check if multiple tables needed
    if any(word in query for word in ["join", "with", "and"]):
        complexity = "complex"
    
    state["query_metadata"] = {
        "type": query_type,
        "complexity": complexity,
        "original_query": state["query"]
    }
    
    return state


def generate_sql(state: AgentState) -> AgentState:
    """
    Node 6.2: Generate SQL query from natural language
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state with generated SQL query
    """
    _, schema_service, llm_service = _get_services()
    
    if llm_service is None:
        state["error"] = "LLM service not available. Please set GOOGLE_API_KEY."
        return state
    
    try:
        schema_context = schema_service.build_schema_context(include_examples=True)
        sql = llm_service.generate_sql(state["query"], schema_context)
        state["sql_query"] = sql
    except Exception as e:
        state["error"] = f"SQL generation failed: {str(e)}"
    
    return state


def validate_sql(state: AgentState) -> AgentState:
    """
    Node 6.3: Validate SQL query for safety and BigQuery correctness
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state (with error if validation fails)
    """
    if state.get("error"):
        return state  # Skip if previous step failed
    
    sql = state.get("sql_query", "")
    sql_upper = sql.upper()
    
    # Check for destructive operations (using word boundaries to avoid false positives)
    forbidden_keywords = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
    for keyword in forbidden_keywords:
        # Use word boundary regex to match whole words only
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, sql):
            state["error"] = f"Query contains forbidden operation: {keyword}"
            return state
    
    # Check for SELECT
    if "SELECT" not in sql:
        state["error"] = "Query must be a SELECT statement"
        return state
    
    # Check for dataset reference
    if config.DATASET_ID.replace("-", "_") not in sql.replace("-", "_"):
        # Warn but don't fail - might be using table aliases
        pass
    
    # BigQuery-specific syntax validation (warnings only, don't fail)
    # Let error recovery handle actual fixes
    bigquery_issues = []
    
    # Check for DATEDIFF (should be DATE_DIFF)
    if re.search(r'\bDATEDIFF\b', sql, re.IGNORECASE):
        bigquery_issues.append("Use DATE_DIFF instead of DATEDIFF")
    
    # Note: DATE_DIFF type mismatches are complex to validate statically
    # They will be caught by BigQuery and handled by error recovery
    
    if bigquery_issues:
        # Log as debug info - error recovery will handle it
        pass
    
    return state


def execute_query(state: AgentState) -> AgentState:
    """
    Node 6.4: Execute SQL query on BigQuery with automatic error recovery
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state with query results
    """
    if state.get("error"):
        return state  # Skip if previous step failed
    
    max_retries = 2
    retry_count = state.get("retry_count", 0)
    
    if retry_count >= max_retries:
        return state  # Give up after max retries
    
    bigquery_service, schema_service, llm_service = _get_services()
    
    try:
        sql = state["sql_query"]
        result = bigquery_service.execute_query(sql, limit_rows=config.MAX_QUERY_ROWS)
        state["query_result"] = result
        # Clear error and reset retry count on success
        if state.get("error"):
            state["error"] = None
        state["retry_count"] = 0
    except Exception as e:
        error_msg = str(e)
        state["error"] = f"Query execution failed: {error_msg}"
        
        # Attempt error recovery if retries available
        if retry_count < max_retries:
            state = _recover_from_error(state, error_msg, schema_service, llm_service)
            state["retry_count"] = retry_count + 1
            # Retry execution
            return execute_query(state)
    
    return state


def _recover_from_error(
    state: AgentState,
    error_msg: str,
    schema_service,
    llm_service
) -> AgentState:
    """
    Attempt to fix SQL based on error message
    
    Args:
        state: Current agent state with error
        error_msg: Error message from BigQuery
        schema_service: Schema service instance
        llm_service: LLM service instance
        
    Returns:
        Updated state with corrected SQL (or original error if recovery fails)
    """
    failed_sql = state.get("sql_query", "")
    user_query = state.get("query", "")
    
    # Track previous errors
    if state.get("previous_errors") is None:
        state["previous_errors"] = []
    state["previous_errors"].append(error_msg)
    
    # Simple pattern-based fixes
    corrected_sql = failed_sql
    
    # Fix 1: DATEDIFF → DATE_DIFF
    if "DATEDIFF" in error_msg.upper() or re.search(r'\bDATEDIFF\b', failed_sql, re.IGNORECASE):
        corrected_sql = re.sub(r'\bDATEDIFF\b', 'DATE_DIFF', corrected_sql, flags=re.IGNORECASE)
        # Fix the function signature: DATE_DIFF(date1, date2, date_part)
        # This is a simple fix, may need more sophisticated handling
    
    # Fix 2: TIMESTAMP vs DATE mismatch or DATE_DIFF with TIMESTAMP
    if ("TIMESTAMP" in error_msg and "DATE" in error_msg) or "DATE_DIFF does not support" in error_msg:
        # DATE_DIFF requires DATE type, not TIMESTAMP - use LLM for complex fix
        if llm_service and schema_service:
            try:
                schema_context = schema_service.build_schema_context(include_examples=False)
                from prompts.sql_generation import get_error_recovery_prompt
                recovery_prompt = get_error_recovery_prompt(
                    user_query,
                    failed_sql,
                    error_msg,
                    schema_context
                )
                corrected_sql = llm_service.generate_text(recovery_prompt)
                # Clean SQL output
                corrected_sql = corrected_sql.strip()
                if corrected_sql.startswith("```sql"):
                    corrected_sql = corrected_sql[6:]
                elif corrected_sql.startswith("```"):
                    corrected_sql = corrected_sql[3:]
                if corrected_sql.endswith("```"):
                    corrected_sql = corrected_sql[:-3]
                corrected_sql = corrected_sql.strip()
            except Exception as e:
                # If LLM recovery fails, keep original error
                return state
    
    # Fix 3: Column not found - use LLM for complex fixes
    elif "not found" in error_msg.lower() or "column" in error_msg.lower():
        # Use LLM for column location fixes
        if llm_service and schema_service:
            try:
                schema_context = schema_service.build_schema_context(include_examples=False)
                from prompts.sql_generation import get_error_recovery_prompt
                recovery_prompt = get_error_recovery_prompt(
                    user_query,
                    failed_sql,
                    error_msg,
                    schema_context
                )
                corrected_sql = llm_service.generate_text(recovery_prompt)
                # Clean SQL output
                corrected_sql = corrected_sql.strip()
                if corrected_sql.startswith("```sql"):
                    corrected_sql = corrected_sql[6:]
                elif corrected_sql.startswith("```"):
                    corrected_sql = corrected_sql[3:]
                if corrected_sql.endswith("```"):
                    corrected_sql = corrected_sql[:-3]
                corrected_sql = corrected_sql.strip()
            except Exception as e:
                # If LLM recovery fails, keep original error
                return state
    
    # Update state with corrected SQL
    state["sql_query"] = corrected_sql
    state["error"] = None  # Clear error to allow retry
    
    return state


def analyze_results(state: AgentState) -> AgentState:
    """
    Node 6.5: Generate business insights from query results
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state with insights
    """
    if state.get("error") or state.get("query_result") is None:
        return state  # Skip if previous step failed
    
    _, _, llm_service = _get_services()
    
    try:
        from utils.formatters import format_query_result
        from prompts.insight_generation import get_insight_generation_prompt, get_simple_insight_template
        
        df = state["query_result"]
        query_type = state.get("query_metadata", {}).get("type") if state.get("query_metadata") else None
        
        if llm_service is None:
            # Generate simple insights without LLM
            template = get_simple_insight_template(query_type)
            state["insights"] = template.format(
                count=len(df),
                rows=len(df),
                cols=len(df.columns),
                entity="items"
            )
            return state
        
        # Create summary of results
        result_summary = format_query_result(df, max_rows=10)
        
        # Generate insights with LLM using prompt template
        insight_prompt = get_insight_generation_prompt(
            user_query=state['query'],
            query_result_summary=result_summary,
            query_type=query_type
        )
        
        insights = llm_service.generate_text(insight_prompt)
        state["insights"] = insights
        
    except Exception as e:
        state["error"] = f"Insight generation failed: {str(e)}"
    
    return state


def create_visualization(state: AgentState) -> AgentState:
    """
    Node 6.6: Create visualization specification from results
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state with visualization spec
    """
    if state.get("error") or state.get("query_result") is None:
        return state  # Skip if previous step failed
    
    try:
        from services.visualization_service import VisualizationService
        
        df = state["query_result"]
        query_type = state.get("query_metadata", {}).get("type") if state.get("query_metadata") else None
        
        viz_service = VisualizationService()
        viz_spec = viz_service.create_visualization(
            df=df,
            query_type=query_type,
            title=state.get("query", "Query Results")
        )
        
        state["visualization_spec"] = viz_spec
        
    except Exception as e:
        # Visualization is optional, don't fail
        state["visualization_spec"] = None
    
    return state


def format_response(state: AgentState) -> AgentState:
    """
    Node 6.7: Format final response for display
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state with formatted response
    """
    try:
        from utils.formatters import format_agent_response
        
        # Format the response (can be used by Streamlit or other interfaces)
        formatted = format_agent_response(state)
        state["formatted_response"] = formatted
        
    except Exception as e:
        # Formatting is optional, don't fail
        pass
    
    return state
