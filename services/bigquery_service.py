"""BigQuery service for query execution and schema introspection"""
import os
import subprocess
from typing import Optional, List, Dict, Any
import pandas as pd
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.exceptions import GoogleCloudError
import time
import hashlib

import config
from services.cache_service import CacheService
from utils.query_optimizer import QueryOptimizer
from utils.logger import ComponentLogger
from utils.tracing import trace_span
from utils.request_context import RequestContext


class BigQueryService:
    """Service for interacting with BigQuery"""
    
    def __init__(self, project_id: Optional[str] = None, enable_cache: bool = True):
        """
        Initialize BigQuery client
        
        Args:
            project_id: Optional project ID. If None, tries to get from gcloud config or env
            enable_cache: Whether to enable query result caching
        """
        self.logger = ComponentLogger("bigquery_service")
        self.project_id = project_id or self._get_project_id()
        self.client = self._initialize_client()
        self.dataset_id = config.DATASET_ID
        self.max_rows = config.MAX_QUERY_ROWS
        self.enable_cache = enable_cache
        self.cache_service = CacheService() if enable_cache else None
        self.query_optimizer = QueryOptimizer()
        
        self.logger.info(
            "BigQuery service initialized",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            cache_enabled=enable_cache
        )
        
    def _get_project_id(self) -> Optional[str]:
        """Get project ID from gcloud config or environment"""
        try:
            result = subprocess.run(
                ['gcloud', 'config', 'get-value', 'project'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0 and result.stdout.strip():
                project_id = result.stdout.strip()
                os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
                return project_id
        except Exception:
            pass
        return os.environ.get("GOOGLE_CLOUD_PROJECT")
    
    def _initialize_client(self) -> bigquery.Client:
        """Initialize BigQuery client with error handling"""
        try:
            if self.project_id:
                client = bigquery.Client(project=self.project_id)
            else:
                client = bigquery.Client()
            self.logger.debug("BigQuery client initialized", project_id=self.project_id)
            return client
        except DefaultCredentialsError as e:
            self.logger.error(
                "Failed to initialize BigQuery client: credentials not found",
                error=str(e)
            )
            raise RuntimeError(
                "Google Cloud credentials not found. "
                "Please run 'gcloud auth application-default login'"
            )
    
    def execute_query(
        self,
        query: str,
        limit_rows: Optional[int] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Execute a BigQuery SQL query and return results as DataFrame
        
        Args:
            query: SQL query string
            limit_rows: Optional limit on number of rows to return
            use_cache: Whether to use cached results if available
            
        Returns:
            DataFrame with query results
            
        Raises:
            RuntimeError: If query execution fails
        """
        # Generate query hash for logging
        query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
        
        with trace_span("execute_query", component="bigquery_service", query_hash=query_hash) as span:
            # Check cache first
            if self.enable_cache and use_cache and self.cache_service:
                cached_result = self.cache_service.get_cached_result(query, limit_rows=limit_rows)
                if cached_result is not None:
                    self.logger.info(
                        "Query result retrieved from cache",
                        query_hash=query_hash,
                        rows=len(cached_result),
                        cache_hit=True
                    )
                    span.set_tag("cache_hit", True)
                    span.set_tag("rows_returned", len(cached_result))
                    return cached_result
            
            span.set_tag("cache_hit", False)
            
            try:
                # Get optimization suggestions
                suggestions = self.query_optimizer.suggest_optimizations(query)
                if suggestions:
                    self.logger.debug(
                        "Query optimization suggestions",
                        query_hash=query_hash,
                        suggestions=suggestions
                    )
                
                # Estimate cost
                cost_info = self.query_optimizer.estimate_query_cost(query)
                span.set_tag("estimated_cost_usd", cost_info.get("estimated_cost_usd", 0))
                span.set_tag("complexity", cost_info.get("complexity", "unknown"))
                
                # Add LIMIT if not present and limit_rows is specified
                if limit_rows and "LIMIT" not in query.upper():
                    query = f"{query.rstrip(';')} LIMIT {limit_rows}"
                
                self.logger.info(
                    "Executing BigQuery query",
                    query_hash=query_hash,
                    estimated_cost_usd=cost_info.get("estimated_cost_usd", 0),
                    complexity=cost_info.get("complexity", "unknown")
                )
                
                start_time = time.time()
                
                # Execute query
                query_job = self.client.query(query)
                rows = query_job.result()
                
                # Convert to DataFrame
                df = rows.to_dataframe()
                
                execution_time = (time.time() - start_time) * 1000  # Convert to ms
                
                # Apply additional limit if needed
                if limit_rows and len(df) > limit_rows:
                    df = df.head(limit_rows)
                
                # Cache result
                if self.enable_cache and use_cache and self.cache_service:
                    self.cache_service.cache_result(query, df, limit_rows=limit_rows)
                
                self.logger.info(
                    "Query executed successfully",
                    query_hash=query_hash,
                    rows_returned=len(df),
                    execution_time_ms=execution_time,
                    cache_hit=False
                )
                
                span.set_tag("rows_returned", len(df))
                span.set_tag("execution_time_ms", execution_time)
                span.log("Query completed successfully", level="INFO")
                
                return df
                
            except GoogleCloudError as e:
                error_msg = str(e)
                self.logger.error(
                    "BigQuery error during query execution",
                    query_hash=query_hash,
                    error=error_msg,
                    error_type=type(e).__name__
                )
                span.set_tag("error", True)
                span.set_tag("error_type", type(e).__name__)
                span.log(f"BigQuery error: {error_msg}", level="ERROR")
                raise RuntimeError(f"BigQuery error: {error_msg}")
            except Exception as e:
                error_msg = str(e)
                self.logger.error(
                    "Query execution failed",
                    query_hash=query_hash,
                    error=error_msg,
                    error_type=type(e).__name__
                )
                span.set_tag("error", True)
                span.set_tag("error_type", type(e).__name__)
                span.log(f"Query execution failed: {error_msg}", level="ERROR")
                raise RuntimeError(f"Query execution failed: {error_msg}")
    
    def estimate_query_cost(self, query: str) -> Dict[str, Any]:
        """
        Estimate the cost of executing a query
        
        Args:
            query: SQL query string
            
        Returns:
            Dictionary with cost estimation information
        """
        return self.query_optimizer.estimate_query_cost(query)
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get schema information for a table
        
        Args:
            table_name: Name of the table (e.g., 'orders')
            
        Returns:
            List of dictionaries with field information
        """
        with trace_span("get_table_schema", component="bigquery_service", table_name=table_name):
            try:
                self.logger.debug("Fetching table schema", table_name=table_name)
                
                table_ref = self.client.dataset(
                    self.dataset_id.split('.')[1],
                    project=self.dataset_id.split('.')[0]
                ).table(table_name)
                
                table = self.client.get_table(table_ref)
                
                schema = []
                for field in table.schema:
                    schema.append({
                        'name': field.name,
                        'type': field.field_type,
                        'mode': field.mode,
                        'description': field.description or ''
                    })
                
                self.logger.debug(
                    "Table schema retrieved",
                    table_name=table_name,
                    column_count=len(schema)
                )
                
                return schema
                
            except Exception as e:
                error_msg = f"Failed to get schema for {table_name}: {str(e)}"
                self.logger.error(
                    "Failed to get table schema",
                    table_name=table_name,
                    error=str(e),
                    error_type=type(e).__name__
                )
                raise RuntimeError(error_msg)
    
    def get_table_names(self) -> List[str]:
        """Get list of all tables in the dataset"""
        return config.REQUIRED_TABLES
    
    def test_connection(self) -> bool:
        """
        Test BigQuery connection with a simple query
        
        Returns:
            True if connection works, False otherwise
        """
        try:
            self.logger.info("Testing BigQuery connection")
            test_query = f"SELECT COUNT(*) as count FROM `{self.dataset_id}.orders`"
            result = self.execute_query(test_query)
            success = len(result) > 0 and len(result.columns) > 0
            if success:
                self.logger.info("BigQuery connection test successful")
            else:
                self.logger.warning("BigQuery connection test returned empty result")
            return success
        except Exception as e:
            self.logger.error(
                "BigQuery connection test failed",
                error=str(e),
                error_type=type(e).__name__
            )
            return False

