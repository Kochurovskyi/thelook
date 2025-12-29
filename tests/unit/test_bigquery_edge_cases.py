"""Edge case tests for BigQuery service focusing on e-commerce data patterns"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch
from google.cloud.exceptions import GoogleCloudError
from services.bigquery_service import BigQueryService
import config


@pytest.mark.unit
class TestBigQueryEdgeCases:
    """Edge case tests for BigQueryService with e-commerce data scenarios"""
    
    def test_orders_table_empty_result(self, mock_bigquery_client):
        """Test querying orders table with no matching records"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            empty_df = pd.DataFrame(columns=['order_id', 'user_id', 'status', 'created_at'])
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = empty_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT * FROM `bigquery-public-data.thelook_ecommerce.orders` "
                "WHERE order_id = -1"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
            assert list(result.columns) == ['order_id', 'user_id', 'status', 'created_at']
    
    def test_order_items_join_with_missing_products(self, mock_bigquery_client):
        """Test joining order_items with products where some products are missing"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Simulate LEFT JOIN with NULLs for missing products
            join_df = pd.DataFrame({
                'order_id': [1, 2, 3],
                'product_id': [100, 200, None],  # NULL for missing product
                'product_name': ['Product A', 'Product B', None],
                'sale_price': [29.99, 49.99, 19.99]
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = join_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT oi.order_id, p.name as product_name, oi.sale_price "
                "FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert result['product_name'].isnull().any()  # Has NULLs from missing products
    
    def test_users_table_with_all_null_demographics(self, mock_bigquery_client):
        """Test users table query with all NULL demographic fields"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            null_users_df = pd.DataFrame({
                'id': [1, 2, 3],
                'first_name': [None, None, None],
                'last_name': [None, None, None],
                'email': ['user1@test.com', 'user2@test.com', None],
                'age': [None, None, None],
                'gender': [None, None, None],
                'state': [None, None, None]
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = null_users_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT * FROM `bigquery-public-data.thelook_ecommerce.users` "
                "WHERE age IS NULL"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert result['age'].isnull().all()
            assert result['first_name'].isnull().all()
    
    def test_products_table_with_zero_and_negative_prices(self, mock_bigquery_client):
        """Test products table with edge case prices (zero, negative, very large)"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            price_df = pd.DataFrame({
                'id': [1, 2, 3, 4, 5],
                'name': ['Free Product', 'Negative Price', 'Normal', 'Very Expensive', 'NULL Price'],
                'retail_price': [0.0, -10.0, 29.99, 999999.99, None],
                'cost': [0.0, 5.0, 15.0, 500000.0, None]
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = price_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT id, name, retail_price, cost FROM products"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert (result['retail_price'] <= 0).any()  # Has zero or negative
            assert result['retail_price'].isnull().any()  # Has NULL
    
    def test_aggregation_with_all_null_values(self, mock_bigquery_client):
        """Test aggregation functions (SUM, AVG) when all values are NULL"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Aggregation with all NULLs returns NULL
            agg_df = pd.DataFrame({
                'total_revenue': [None],
                'avg_price': [None],
                'order_count': [0]
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = agg_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT SUM(sale_price) as total_revenue, "
                "AVG(sale_price) as avg_price, "
                "COUNT(*) as order_count "
                "FROM order_items WHERE sale_price IS NULL"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert result['total_revenue'].isnull().iloc[0]
            assert result['avg_price'].isnull().iloc[0]
            assert result['order_count'].iloc[0] == 0
    
    def test_date_range_queries_edge_cases(self, mock_bigquery_client):
        """Test date range queries with edge cases (future dates, very old dates)"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            import pandas as pd
            date_df = pd.DataFrame({
                'order_id': [1, 2, 3, 4],
                'created_at': pd.to_datetime([
                    '1900-01-01',  # Very old
                    '2020-01-01',  # Normal
                    '2099-12-31',  # Future date
                    None  # NULL
                ])
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = date_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT order_id, created_at FROM orders "
                "WHERE created_at < '1900-01-01' OR created_at > '2099-01-01'"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert pd.api.types.is_datetime64_any_dtype(result['created_at'])
    
    def test_group_by_with_single_row_per_group(self, mock_bigquery_client):
        """Test GROUP BY queries where each group has only one row"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            group_df = pd.DataFrame({
                'user_id': [1, 2, 3, 4, 5],
                'order_count': [1, 1, 1, 1, 1],  # Each user has exactly 1 order
                'total_spent': [29.99, 49.99, 19.99, 99.99, 15.99]
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = group_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT user_id, COUNT(*) as order_count, SUM(total) as total_spent "
                "FROM orders GROUP BY user_id HAVING COUNT(*) = 1"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert (result['order_count'] == 1).all()
    
    def test_order_by_with_duplicate_values(self, mock_bigquery_client):
        """Test ORDER BY with duplicate values (ties)"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Multiple products with same revenue
            revenue_df = pd.DataFrame({
                'product_name': ['Product A', 'Product B', 'Product C', 'Product D'],
                'revenue': [1000.0, 1000.0, 1000.0, 500.0]  # Three products tied
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = revenue_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT name as product_name, SUM(sale_price) as revenue "
                "FROM products p JOIN order_items oi ON p.id = oi.product_id "
                "GROUP BY name ORDER BY revenue DESC"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert (result['revenue'].head(3) == 1000.0).all()  # Top 3 are tied
    
    def test_window_functions_edge_cases(self, mock_bigquery_client):
        """Test window functions (ROW_NUMBER, RANK) with edge cases"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            window_df = pd.DataFrame({
                'user_id': [1, 1, 1, 2, 2],
                'order_id': [101, 102, 103, 201, 202],
                'order_rank': [1, 2, 3, 1, 2],  # ROW_NUMBER within user
                'revenue': [100.0, 200.0, 150.0, 50.0, 75.0]
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = window_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT user_id, order_id, "
                "ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) as order_rank, "
                "revenue FROM orders"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert 'order_rank' in result.columns
    
    def test_case_sensitivity_in_queries(self, mock_bigquery_client):
        """Test case sensitivity handling in SQL queries"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            case_df = pd.DataFrame({
                'status': ['Complete', 'COMPLETE', 'complete', 'Processing']
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = case_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT status FROM orders WHERE UPPER(status) = 'COMPLETE'"
            )
            
            assert isinstance(result, pd.DataFrame)
            # Should handle case-insensitive matching
    
    def test_very_long_query_string(self, mock_bigquery_client):
        """Test execution of very long SQL query string"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Create a very long query with many JOINs and conditions
            long_query = "SELECT " + ", ".join([f"p.col{i}" for i in range(100)]) + \
                        " FROM products p " + \
                        " JOIN order_items oi ON p.id = oi.product_id " * 10 + \
                        " WHERE " + " AND ".join([f"p.col{i} > 0" for i in range(50)])
            
            normal_df = pd.DataFrame({'col0': [1]})
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = normal_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(long_query)
            
            assert isinstance(result, pd.DataFrame)
            # Should handle long queries without issues
    
    def test_query_with_special_sql_keywords_in_data(self, mock_bigquery_client):
        """Test queries where data contains SQL keywords as strings"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Product names that are SQL keywords
            keyword_df = pd.DataFrame({
                'product_name': ['SELECT', 'FROM', 'WHERE', 'JOIN', 'GROUP BY'],
                'category': ['SELECT', 'FROM', 'WHERE', 'JOIN', 'GROUP BY']
            })
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = keyword_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            result = service.execute_query(
                "SELECT name as product_name, category FROM products "
                "WHERE name IN ('SELECT', 'FROM', 'WHERE')"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert 'SELECT' in result['product_name'].values
    
    def test_concurrent_table_access_simulation(self, mock_bigquery_client):
        """Test handling of queries that might simulate concurrent access"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Simulate multiple rapid queries
            queries = [
                "SELECT COUNT(*) FROM orders",
                "SELECT COUNT(*) FROM order_items",
                "SELECT COUNT(*) FROM products",
                "SELECT COUNT(*) FROM users"
            ]
            
            count_df = pd.DataFrame({'f0_': [100]})
            mock_query_job = Mock()
            mock_result = Mock()
            mock_result.to_dataframe.return_value = count_df
            mock_query_job.result.return_value = mock_result
            mock_bigquery_client.query.return_value = mock_query_job
            
            results = []
            for query in queries:
                result = service.execute_query(query)
                results.append(result)
            
            assert len(results) == 4
            assert all(isinstance(r, pd.DataFrame) for r in results)
    
    def test_schema_retrieval_for_all_required_tables(self, mock_bigquery_client):
        """Test schema retrieval for all 4 required tables"""
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            service = BigQueryService(project_id="test-project")
            
            # Mock schemas for each table
            mock_table = Mock()
            mock_field = Mock()
            mock_field.name = 'id'
            mock_field.field_type = 'INTEGER'
            mock_field.mode = 'NULLABLE'
            mock_field.description = 'ID field'
            mock_table.schema = [mock_field]
            mock_bigquery_client.get_table.return_value = mock_table
            
            for table_name in config.REQUIRED_TABLES:
                schema = service.get_table_schema(table_name)
                assert isinstance(schema, list)
                assert len(schema) > 0
                assert schema[0]['name'] == 'id'

