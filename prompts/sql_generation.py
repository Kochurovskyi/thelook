"""SQL generation prompts for LLM with dynamic assembly and few-shot examples"""
from typing import Optional, Dict, List


def get_sql_generation_prompt(
    user_query: str,
    schema_context: str,
    few_shot_examples: Optional[str] = None
) -> str:
    """
    Build SQL generation prompt with schema context
    
    Args:
        user_query: User's natural language query
        schema_context: Formatted database schema context
        few_shot_examples: Optional few-shot examples
        
    Returns:
        Complete prompt for SQL generation
    """
    base_prompt = f"""You are a SQL expert. Generate a BigQuery SQL query based on the user's question.

{schema_context}

BIGQUERY-SPECIFIC FUNCTIONS (CRITICAL - Use these exact function names):
- Use DATE_DIFF(date1, date2, date_part) NOT DATEDIFF
- Use DATE_ADD(date, INTERVAL int64 date_part) with proper types
- Use DATE_TRUNC(date, date_part) for date truncation
- Use CAST() or SAFE_CAST() for type conversions
- TIMESTAMP and DATE are different types - use CAST when comparing: CAST(timestamp_col AS DATE)
- Use EXTRACT(date_part FROM date) for date extraction
- Use DATE() function to convert TIMESTAMP to DATE: DATE(created_at)
- Use DATE_SUB(date, INTERVAL int64 date_part) for date subtraction

SAFETY RULES:
- Only use SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)
- Always use fully qualified table names: `bigquery-public-data.thelook_ecommerce.<table_name>`
- Limit results to reasonable sizes (use LIMIT when appropriate)
- Use proper JOIN syntax when combining tables
- Handle NULL values appropriately
- When comparing dates/timestamps, ensure types match (use CAST if needed)

USER QUESTION: {user_query}

Generate a SQL query that answers this question. Return ONLY the SQL query, no explanations.
"""
    
    if few_shot_examples:
        base_prompt = f"""{base_prompt}

EXAMPLES:
{few_shot_examples}
"""
    
    return base_prompt


def get_few_shot_examples(query_type: Optional[str] = None) -> str:
    """
    Get few-shot examples for SQL generation, optionally filtered by query type
    
    Args:
        query_type: Optional query type to filter examples (count, ranking, aggregation, etc.)
        
    Returns:
        String with few-shot examples
    """
    all_examples = {
        "count": """Example 1:
Question: Count all orders
SQL: SELECT COUNT(*) as order_count FROM `bigquery-public-data.thelook_ecommerce.orders`

Example 2:
Question: How many users are there?
SQL: SELECT COUNT(DISTINCT id) as user_count FROM `bigquery-public-data.thelook_ecommerce.users`

Example 3:
Question: Count orders by status
SQL: SELECT status, COUNT(*) as count FROM `bigquery-public-data.thelook_ecommerce.orders` GROUP BY status""",
        
        "ranking": """Example 1:
Question: Show top 10 products by revenue
SQL: 
SELECT 
    p.name,
    SUM(oi.sale_price) as revenue
FROM `bigquery-public-data.thelook_ecommerce.products` p
JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON p.id = oi.product_id
GROUP BY p.name
ORDER BY revenue DESC
LIMIT 10

Example 2:
Question: Top 5 customers by total spending
SQL:
SELECT 
    u.id,
    u.first_name,
    u.last_name,
    SUM(oi.sale_price) as total_spent
FROM `bigquery-public-data.thelook_ecommerce.users` u
JOIN `bigquery-public-data.thelook_ecommerce.orders` o ON u.id = o.user_id
JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON o.order_id = oi.order_id
GROUP BY u.id, u.first_name, u.last_name
ORDER BY total_spent DESC
LIMIT 5""",
        
        "aggregation": """Example 1:
Question: What is the average order value?
SQL:
SELECT 
    AVG(order_total) as avg_order_value
FROM (
    SELECT 
        o.order_id,
        SUM(oi.sale_price) as order_total
    FROM `bigquery-public-data.thelook_ecommerce.orders` o
    JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON o.order_id = oi.order_id
    GROUP BY o.order_id
)

Example 2:
Question: Average product price
SQL: SELECT AVG(retail_price) as avg_price FROM `bigquery-public-data.thelook_ecommerce.products`""",
        
        "temporal": """Example 1: Sales trends over time period
Question: Show sales trends over the last 12 months
SQL:
SELECT 
    DATE_TRUNC(DATE(created_at), MONTH) as month,
    SUM(sale_price) as total_sales
FROM `bigquery-public-data.thelook_ecommerce.order_items`
WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
GROUP BY month
ORDER BY month

Example 2: Date differences
Question: Calculate days between order and delivery
SQL:
SELECT 
    order_id,
    DATE_DIFF(DATE(delivered_at), DATE(created_at), DAY) as days_to_deliver
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE delivered_at IS NOT NULL

Example 3: Sales by month
SQL:
SELECT 
    DATE_TRUNC(DATE(created_at), MONTH) as month,
    SUM(oi.sale_price) as total_sales
FROM `bigquery-public-data.thelook_ecommerce.orders` o
JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON o.order_id = oi.order_id
GROUP BY month
ORDER BY month

Example 4: Daily order count
SQL:
SELECT 
    DATE(created_at) as date,
    COUNT(*) as order_count
FROM `bigquery-public-data.thelook_ecommerce.orders`
GROUP BY date
ORDER BY date DESC""",
        
        "customer_analysis": """Example 1:
Question: Customer segments by order count
SQL:
SELECT 
    CASE 
        WHEN order_count >= 10 THEN 'High Value'
        WHEN order_count >= 5 THEN 'Medium Value'
        ELSE 'Low Value'
    END as segment,
    COUNT(*) as customer_count
FROM (
    SELECT user_id, COUNT(*) as order_count
    FROM `bigquery-public-data.thelook_ecommerce.orders`
    GROUP BY user_id
)
GROUP BY segment""",
        
        "product_analysis": """Example 1:
Question: Products with low inventory
SQL:
SELECT 
    name,
    category,
    retail_price
FROM `bigquery-public-data.thelook_ecommerce.products`
WHERE cost > retail_price * 0.8
ORDER BY retail_price DESC""",
        
        "geographic": """Example 1: Sales by country
Question: Show sales by country
SQL:
SELECT 
    u.country,
    SUM(oi.sale_price) as total_sales
FROM `bigquery-public-data.thelook_ecommerce.users` u
JOIN `bigquery-public-data.thelook_ecommerce.orders` o ON u.id = o.user_id
JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON o.order_id = oi.order_id
GROUP BY u.country
ORDER BY total_sales DESC

IMPORTANT: The 'country' column is in the 'users' table, NOT in the 'orders' table.
You must JOIN users table to access country information.

Example 2: Regional performance comparison
Question: Compare sales performance across different countries
SQL:
SELECT 
    u.country,
    COUNT(DISTINCT o.order_id) as order_count,
    SUM(oi.sale_price) as total_revenue,
    AVG(oi.sale_price) as avg_order_value
FROM `bigquery-public-data.thelook_ecommerce.users` u
JOIN `bigquery-public-data.thelook_ecommerce.orders` o ON u.id = o.user_id
JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi ON o.order_id = oi.order_id
GROUP BY u.country
ORDER BY total_revenue DESC"""
    }
    
    if query_type and query_type in all_examples:
        return all_examples[query_type]
    
    # Return all examples if no type specified
    return "\n\n".join(all_examples.values())


def build_dynamic_prompt(
    user_query: str,
    schema_context: str,
    query_metadata: Optional[Dict] = None,
    previous_errors: Optional[List[str]] = None
) -> str:
    """
    Build a dynamic SQL generation prompt with context-aware assembly
    
    Args:
        user_query: User's natural language query
        schema_context: Formatted database schema context
        query_metadata: Optional metadata about the query (type, complexity)
        previous_errors: Optional list of previous errors to help with recovery
        
    Returns:
        Complete prompt for SQL generation
    """
    # Base prompt
    prompt_parts = ["You are a SQL expert. Generate a BigQuery SQL query based on the user's question."]
    
    # Add schema context
    prompt_parts.append(f"\n{schema_context}")
    
    # Add query type-specific guidance
    if query_metadata:
        query_type = query_metadata.get("type", "general")
        complexity = query_metadata.get("complexity", "simple")
        
        if query_type != "general":
            prompt_parts.append(f"\nQUERY TYPE: {query_type.upper()}")
            if complexity == "complex":
                prompt_parts.append("NOTE: This is a complex query. Consider using CTEs or subqueries for clarity.")
    
    # Add error recovery context if previous errors exist
    if previous_errors:
        prompt_parts.append("\nPREVIOUS ERRORS TO AVOID:")
        for i, error in enumerate(previous_errors, 1):
            prompt_parts.append(f"{i}. {error}")
        prompt_parts.append("Please fix these issues in your SQL query.")
    
    # Add safety rules
    prompt_parts.append("""
SAFETY RULES:
- Only use SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)
- Always use fully qualified table names: `bigquery-public-data.thelook_ecommerce.<table_name>`
- Limit results to reasonable sizes (use LIMIT when appropriate)
- Use proper JOIN syntax when combining tables
- Handle NULL values appropriately
- Use appropriate data types in WHERE clauses
""")
    
    # Add few-shot examples based on query type
    query_type = query_metadata.get("type") if query_metadata else None
    examples = get_few_shot_examples(query_type)
    if examples:
        prompt_parts.append(f"\nEXAMPLES:\n{examples}")
    
    # Add user question
    prompt_parts.append(f"\nUSER QUESTION: {user_query}")
    
    # Final instruction
    prompt_parts.append("\nGenerate a SQL query that answers this question. Return ONLY the SQL query, no explanations.")
    
    return "\n".join(prompt_parts)


def get_error_recovery_prompt(
    user_query: str,
    failed_query: str,
    error_message: str,
    schema_context: str
) -> str:
    """
    Generate a prompt for error recovery when a query fails
    
    Args:
        user_query: Original user question
        failed_query: The SQL query that failed
        error_message: Error message from BigQuery
        schema_context: Formatted database schema context
        
    Returns:
        Prompt for generating a corrected query
    """
    return f"""You are a SQL expert. A previous SQL query failed with an error. Please generate a corrected query.

ORIGINAL USER QUESTION: {user_query}

FAILED QUERY:
{failed_query}

ERROR MESSAGE:
{error_message}

{schema_context}

INSTRUCTIONS:
1. Analyze the error message to understand what went wrong
2. Common issues:
   - Column name typos or incorrect table references
   - Missing JOIN conditions
   - Data type mismatches in WHERE clauses
   - Missing table aliases
   - Incorrect aggregate function usage
3. Generate a corrected SQL query that fixes the error
4. Ensure the query still answers the original user question

Return ONLY the corrected SQL query, no explanations.
"""

