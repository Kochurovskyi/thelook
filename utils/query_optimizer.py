"""Query optimization utilities for BigQuery"""
import re
from typing import Dict, Any, Optional


class QueryOptimizer:
    """Utilities for optimizing BigQuery SQL queries"""
    
    @staticmethod
    def estimate_query_cost(query: str, table_sizes: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        """
        Estimate BigQuery query cost based on data scanned
        
        Args:
            query: SQL query string
            table_sizes: Optional dictionary mapping table names to row counts
            
        Returns:
            Dictionary with cost estimation information
        """
        # BigQuery pricing: $5 per TB scanned (as of 2024)
        COST_PER_TB = 5.0
        
        # Simple heuristics for cost estimation
        cost_info = {
            "estimated_bytes_scanned": 0,
            "estimated_cost_usd": 0.0,
            "tables_accessed": [],
            "has_joins": False,
            "has_aggregations": False,
            "has_window_functions": False,
            "complexity": "low"
        }
        
        query_upper = query.upper()
        
        # Detect tables accessed
        table_pattern = r'`?bigquery-public-data\.thelook_ecommerce\.(\w+)`?'
        tables = re.findall(table_pattern, query)
        cost_info["tables_accessed"] = list(set(tables))
        
        # Detect joins
        if "JOIN" in query_upper:
            cost_info["has_joins"] = True
            cost_info["complexity"] = "medium"
        
        # Detect aggregations
        if any(op in query_upper for op in ["GROUP BY", "SUM(", "COUNT(", "AVG(", "MAX(", "MIN("]):
            cost_info["has_aggregations"] = True
            if cost_info["complexity"] == "low":
                cost_info["complexity"] = "medium"
        
        # Detect window functions
        if any(func in query_upper for func in ["ROW_NUMBER()", "RANK()", "DENSE_RANK()", "OVER("]):
            cost_info["has_window_functions"] = True
            cost_info["complexity"] = "high"
        
        # Estimate bytes (very rough - would need actual table sizes for accuracy)
        # Assume average row size of 1KB for estimation
        estimated_rows = 10000  # Default assumption
        if table_sizes:
            # Use actual table sizes if provided
            for table in cost_info["tables_accessed"]:
                if table in table_sizes:
                    estimated_rows = max(estimated_rows, table_sizes[table])
        
        # Rough estimation: rows * avg_row_size
        cost_info["estimated_bytes_scanned"] = estimated_rows * 1024  # 1KB per row estimate
        
        # Calculate cost
        cost_info["estimated_cost_usd"] = (cost_info["estimated_bytes_scanned"] / (1024**4)) * COST_PER_TB
        
        return cost_info
    
    @staticmethod
    def add_optimization_hints(query: str) -> str:
        """
        Add optimization hints to SQL query
        
        Args:
            query: Original SQL query
            
        Returns:
            Query with optimization hints (if applicable)
        """
        # For now, just return the query as-is
        # In a real implementation, we could add:
        # - LIMIT clauses where missing
        # - Index hints
        # - Partition pruning hints
        # - Join order optimization
        
        query_upper = query.upper()
        
        # Add LIMIT if missing and query doesn't have aggregations that need all data
        if "LIMIT" not in query_upper and "GROUP BY" not in query_upper:
            # Only add LIMIT for simple SELECT * queries
            if query_upper.strip().startswith("SELECT") and "COUNT(" not in query_upper:
                # Don't modify - let the service handle limits
                pass
        
        return query
    
    @staticmethod
    def suggest_optimizations(query: str) -> list:
        """
        Suggest query optimizations
        
        Args:
            query: SQL query string
            
        Returns:
            List of optimization suggestions
        """
        suggestions = []
        query_upper = query.upper()
        
        # Check for missing LIMIT
        if "LIMIT" not in query_upper and "COUNT(" not in query_upper:
            suggestions.append("Consider adding LIMIT clause to reduce data scanned")
        
        # Check for SELECT *
        if "SELECT *" in query_upper:
            suggestions.append("Consider selecting specific columns instead of * to reduce data scanned")
        
        # Check for multiple JOINs
        join_count = query_upper.count("JOIN")
        if join_count > 3:
            suggestions.append(f"Query has {join_count} JOINs - consider if all are necessary")
        
        # Check for subqueries
        if query_upper.count("SELECT") > 1:
            suggestions.append("Query contains subqueries - consider using CTEs for readability")
        
        # Check for ORDER BY without LIMIT
        if "ORDER BY" in query_upper and "LIMIT" not in query_upper:
            suggestions.append("ORDER BY without LIMIT may scan more data than needed")
        
        return suggestions
    
    @staticmethod
    def validate_query_structure(query: str) -> Dict[str, Any]:
        """
        Validate query structure and provide feedback
        
        Args:
            query: SQL query string
            
        Returns:
            Dictionary with validation results
        """
        validation = {
            "is_valid": True,
            "warnings": [],
            "errors": []
        }
        
        query_upper = query.upper()
        
        # Check for required dataset reference
        if "bigquery-public-data.thelook_ecommerce" not in query:
            validation["warnings"].append("Query should use fully qualified table names")
        
        # Check for forbidden operations
        forbidden = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
        for op in forbidden:
            if f" {op} " in query_upper or query_upper.startswith(op):
                validation["is_valid"] = False
                validation["errors"].append(f"Forbidden operation detected: {op}")
        
        # Check for SELECT
        if "SELECT" not in query_upper:
            validation["is_valid"] = False
            validation["errors"].append("Query must be a SELECT statement")
        
        return validation

