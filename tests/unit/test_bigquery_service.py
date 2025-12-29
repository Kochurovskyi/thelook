"""Unit tests for BigQueryService"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from google.cloud.exceptions import GoogleCloudError
from services.bigquery_service import BigQueryService
import config


@pytest.mark.unit
class TestBigQueryService:
    """Test suite for BigQueryService"""
    
    def test_initialization_with_project_id(self):
        """Test service initialization with project ID"""
        with patch('services.bigquery_service.bigquery.Client') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            
            service = BigQueryService(project_id="test-project")
            
            assert service.project_id == "test-project"
            mock_client_class.assert_called_once_with(project="test-project")
    
    def test_initialization_without_project_id(self):
        """Test service initialization without project ID"""
        with patch('services.bigquery_service.bigquery.Client') as mock_client_class:
            with patch('services.bigquery_service.subprocess.run') as mock_subprocess:
                mock_subprocess.return_value.returncode = 0
                mock_subprocess.return_value.stdout = "test-project\n"
                
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                service = BigQueryService()
                
                assert service.project_id == "test-project"
    
    def test_initialization_credentials_error(self):
        """Test initialization handles credentials error"""
        from google.auth.exceptions import DefaultCredentialsError
        
        with patch('services.bigquery_service.bigquery.Client') as mock_client_class:
            mock_client_class.side_effect = DefaultCredentialsError("No credentials")
            
            with pytest.raises(RuntimeError, match="credentials not found"):
                BigQueryService()
    
    def test_execute_query_success(self, mock_bigquery_client):
        """Test successful query execution"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            query = "SELECT COUNT(*) FROM orders"
            result = service.execute_query(query)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            mock_bigquery_client.query.assert_called_once()
    
    def test_execute_query_with_limit(self, mock_bigquery_client):
        """Test query execution with row limit"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            query = "SELECT * FROM orders"
            result = service.execute_query(query, limit_rows=10)
            
            mock_bigquery_client.query.assert_called_once()
            # Verify LIMIT was added or applied
            call_args = mock_bigquery_client.query.call_args[0][0]
            assert "LIMIT" in call_args.upper() or len(result) <= 10
    
    def test_execute_query_error_handling(self, mock_bigquery_client):
        """Test query execution error handling"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Make query raise an error
            mock_bigquery_client.query.side_effect = GoogleCloudError("Query failed")
            
            with pytest.raises(RuntimeError, match="BigQuery error"):
                service.execute_query("SELECT * FROM invalid_table")
    
    def test_get_table_schema_success(self, mock_bigquery_client):
        """Test successful schema retrieval"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            schema = service.get_table_schema("orders")
            
            assert isinstance(schema, list)
            assert len(schema) > 0
            assert 'name' in schema[0]
            assert 'type' in schema[0]
            mock_bigquery_client.get_table.assert_called_once()
    
    def test_get_table_schema_error(self, mock_bigquery_client):
        """Test schema retrieval error handling"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            mock_bigquery_client.get_table.side_effect = Exception("Table not found")
            
            with pytest.raises(RuntimeError, match="Failed to get schema"):
                service.get_table_schema("nonexistent_table")
    
    def test_get_table_names(self):
        """Test getting table names"""
        with patch('services.bigquery_service.bigquery.Client'):
            service = BigQueryService(project_id="test-project")
            
            tables = service.get_table_names()
            
            assert isinstance(tables, list)
            assert len(tables) == len(config.REQUIRED_TABLES)
            assert all(table in config.REQUIRED_TABLES for table in tables)
    
    def test_test_connection_success(self, mock_bigquery_client):
        """Test connection test success"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Mock successful query
            result = service.test_connection()
            
            assert result is True
    
    def test_test_connection_failure(self, mock_bigquery_client):
        """Test connection test failure"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Make query fail
            mock_bigquery_client.query.side_effect = Exception("Connection failed")
            
            result = service.test_connection()
            
            assert result is False
    
    def test_execute_query_empty_result(self, mock_bigquery_client):
        """Test query execution with empty result"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Mock empty DataFrame
            empty_df = pd.DataFrame()
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = empty_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query("SELECT * FROM orders WHERE 1=0")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_execute_query_with_null_values(self, mock_bigquery_client):
        """Test query execution with NULL values in results"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Mock DataFrame with NULL values
            df_with_nulls = pd.DataFrame({
                'order_id': [1, 2, None],
                'user_id': [10, None, 30],
                'status': ['Complete', 'Processing', None]
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = df_with_nulls
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query("SELECT * FROM orders")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert result.isnull().any().any()  # Has at least one NULL
    
    def test_execute_query_very_large_result(self, mock_bigquery_client):
        """Test query execution with very large result set"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Mock large DataFrame
            large_df = pd.DataFrame({
                'order_id': range(50000),
                'status': ['Complete'] * 50000
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = large_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query("SELECT * FROM orders", limit_rows=1000)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) <= 1000  # Should respect limit
    
    def test_execute_query_sql_injection_attempt(self, mock_bigquery_client):
        """Test query execution with SQL injection attempt"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # SQL injection attempt should still execute (validation happens in validate_sql node)
            malicious_query = "SELECT * FROM orders; DROP TABLE orders; --"
            
            # Should execute but validation node will catch it
            result = service.execute_query(malicious_query)
            
            # Service doesn't validate, just executes
            mock_bigquery_client.query.assert_called_once()
    
    def test_execute_query_timeout_scenario(self, mock_bigquery_client):
        """Test query execution timeout handling"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Simulate timeout
            import time
            mock_query_job = Mock()
            mock_query_job.result.side_effect = lambda: time.sleep(0.1) or Exception("Query timeout")
            mock_bigquery_client.query.return_value = mock_query_job
            
            with pytest.raises(RuntimeError):
                service.execute_query("SELECT * FROM orders")
    
    def test_get_table_schema_empty_table(self, mock_bigquery_client):
        """Test schema retrieval for empty table"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Mock table with no fields
            mock_table = Mock()
            mock_table.schema = []
            mock_bigquery_client.get_table.return_value = mock_table
            
            schema = service.get_table_schema("empty_table")
            
            assert isinstance(schema, list)
            assert len(schema) == 0
    
    def test_get_table_schema_with_repeated_fields(self, mock_bigquery_client):
        """Test schema retrieval with REPEATED (array) fields"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Mock table with REPEATED field
            mock_table = Mock()
            mock_field = Mock()
            mock_field.name = 'tags'
            mock_field.field_type = 'STRING'
            mock_field.mode = 'REPEATED'
            mock_field.description = 'Product tags'
            mock_table.schema = [mock_field]
            mock_bigquery_client.get_table.return_value = mock_table
            
            schema = service.get_table_schema("products")
            
            assert len(schema) == 1
            assert schema[0]['mode'] == 'REPEATED'
    
    def test_execute_query_with_special_characters(self, mock_bigquery_client):
        """Test query execution with special characters in data"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Mock DataFrame with special characters
            special_df = pd.DataFrame({
                'product_name': ['Product "A"', "Product 'B'", 'Product & C'],
                'description': ['Line 1\nLine 2', 'Tab\there', 'Unicode: 测试']
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = special_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query("SELECT * FROM products")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
    
    def test_execute_query_numeric_edge_cases(self, mock_bigquery_client):
        """Test query execution with numeric edge cases"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Mock DataFrame with edge case numbers
            numeric_df = pd.DataFrame({
                'price': [0.0, -1.0, 999999.99, 0.0001, None],
                'quantity': [0, -5, 2147483647, None, 1]
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = numeric_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query("SELECT price, quantity FROM order_items")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 5
            assert result['price'].isnull().any()  # Has NULL
    
    def test_execute_query_date_time_edge_cases(self, mock_bigquery_client):
        """Test query execution with date/time edge cases"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Mock DataFrame with date edge cases
            import pandas as pd
            date_df = pd.DataFrame({
                'created_at': pd.to_datetime([
                    '2020-01-01 00:00:00',
                    '2023-12-31 23:59:59',
                    None,  # NULL date
                    '1970-01-01 00:00:00'  # Epoch
                ]),
                'order_date': pd.to_datetime(['2020-01-01', '2020-12-31', None, '2021-06-15'])
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = date_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query("SELECT created_at, order_date FROM orders")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 4
            assert pd.api.types.is_datetime64_any_dtype(result['created_at'])
    
    def test_execute_query_limit_edge_cases(self, mock_bigquery_client):
        """Test query execution with limit edge cases"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Test with limit=0 (service allows it, just returns empty)
            empty_df = pd.DataFrame()
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = empty_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query("SELECT * FROM orders", limit_rows=0)
            assert isinstance(result, pd.DataFrame)
            
            # Test with very large limit
            large_df = pd.DataFrame({'id': range(1000000)})
            mock_query_job2 = Mock()
            mock_result2 = Mock()
            mock_result2.to_dataframe.return_value = large_df
            mock_query_job2.result.return_value = mock_result2
            mock_bigquery_client.query.return_value = mock_query_job2
            
            result = service.execute_query("SELECT * FROM orders", limit_rows=999999999)
            assert isinstance(result, pd.DataFrame)
    
    def test_get_table_schema_invalid_table_name(self, mock_bigquery_client):
        """Test schema retrieval with invalid table name"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Invalid table name characters
            mock_bigquery_client.get_table.side_effect = Exception("Invalid table name")
            
            with pytest.raises(RuntimeError, match="Failed to get schema"):
                service.get_table_schema("invalid-table-name!")
    
    def test_execute_query_malformed_sql(self, mock_bigquery_client):
        """Test query execution with malformed SQL"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Malformed SQL
            mock_bigquery_client.query.side_effect = GoogleCloudError("Syntax error")
            
            with pytest.raises(RuntimeError, match="BigQuery error"):
                service.execute_query("SELCT * FRM orders")  # Typo in SQL

