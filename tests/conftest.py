"""Pytest configuration and shared fixtures"""
import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock
from google.cloud import bigquery
from agents.state import AgentState, create_initial_state


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client"""
    client = Mock(spec=bigquery.Client)
    
    # Mock query execution
    mock_query_job = Mock()
    mock_result = Mock()
    
    # Create sample DataFrame
    sample_df = pd.DataFrame({
        'count': [125451]
    })
    
    mock_result.to_dataframe.return_value = sample_df
    mock_query_job.result.return_value = mock_result
    client.query.return_value = mock_query_job
    
    # Mock table schema
    mock_table = Mock()
    mock_field1 = Mock()
    mock_field1.name = 'order_id'
    mock_field1.field_type = 'INTEGER'
    mock_field1.mode = 'NULLABLE'
    mock_field1.description = 'Order identifier'
    
    mock_field2 = Mock()
    mock_field2.name = 'user_id'
    mock_field2.field_type = 'INTEGER'
    mock_field2.mode = 'NULLABLE'
    mock_field2.description = 'User identifier'
    
    mock_table.schema = [mock_field1, mock_field2]
    client.get_table.return_value = mock_table
    
    return client


@pytest.fixture
def mock_llm():
    """Mock Gemini LLM"""
    llm = Mock()
    
    # Mock response object
    mock_response = Mock()
    mock_response.content = "SELECT COUNT(*) FROM orders"
    
    llm.invoke.return_value = mock_response
    
    return llm


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing"""
    return pd.DataFrame({
        'product_name': ['Product A', 'Product B', 'Product C'],
        'revenue': [1000.0, 2000.0, 1500.0],
        'orders': [10, 20, 15]
    })


@pytest.fixture
def sample_schema():
    """Sample schema data for testing"""
    return [
        {
            'name': 'order_id',
            'type': 'INTEGER',
            'mode': 'NULLABLE',
            'description': 'Order identifier'
        },
        {
            'name': 'user_id',
            'type': 'INTEGER',
            'mode': 'NULLABLE',
            'description': 'User identifier'
        },
        {
            'name': 'status',
            'type': 'STRING',
            'mode': 'NULLABLE',
            'description': 'Order status'
        }
    ]


@pytest.fixture
def sample_state():
    """Sample agent state for testing"""
    return create_initial_state("Count orders")


@pytest.fixture
def sample_state_with_sql():
    """Sample agent state with SQL query"""
    state = create_initial_state("Count orders")
    state["sql_query"] = "SELECT COUNT(*) as count FROM `bigquery-public-data.thelook_ecommerce.orders`"
    state["query_metadata"] = {
        "type": "count",
        "complexity": "simple"
    }
    return state


@pytest.fixture
def sample_state_with_results():
    """Sample agent state with query results"""
    state = create_initial_state("Count orders")
    state["sql_query"] = "SELECT COUNT(*) as count FROM orders"
    state["query_result"] = pd.DataFrame({'count': [125451]})
    state["query_metadata"] = {
        "type": "count",
        "complexity": "simple"
    }
    return state


@pytest.fixture
def mock_bigquery_service(mock_bigquery_client):
    """Mock BigQueryService"""
    from services.bigquery_service import BigQueryService
    
    service = Mock(spec=BigQueryService)
    service.client = mock_bigquery_client
    service.dataset_id = "bigquery-public-data.thelook_ecommerce"
    service.max_rows = 10000
    
    # Mock methods
    service.execute_query.return_value = pd.DataFrame({'count': [125451]})
    service.get_table_schema.return_value = [
        {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABLE', 'description': ''},
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE', 'description': ''}
    ]
    service.get_table_names.return_value = ["orders", "order_items", "products", "users"]
    service.test_connection.return_value = True
    
    return service


@pytest.fixture
def mock_schema_service(mock_bigquery_service):
    """Mock SchemaService"""
    from services.schema_service import SchemaService
    
    service = Mock(spec=SchemaService)
    service.bigquery_service = mock_bigquery_service
    
    # Mock methods
    service.get_table_schema.return_value = [
        {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABLE', 'description': ''}
    ]
    service.get_all_schemas.return_value = {
        'orders': [{'name': 'order_id', 'type': 'INTEGER'}],
        'order_items': [{'name': 'item_id', 'type': 'INTEGER'}]
    }
    service.build_schema_context.return_value = "DATABASE SCHEMA:\nTable: orders\nColumns: order_id, user_id"
    
    return service


@pytest.fixture
def mock_llm_service(mock_llm):
    """Mock LLMService"""
    from services.llm_service import LLMService
    
    service = Mock(spec=LLMService)
    service.llm = mock_llm
    service.api_key = "test-key"
    service.model_name = "gemini-2.5-flash-lite"
    
    # Mock methods
    service.generate_text.return_value = "Generated text response"
    service.generate_sql.return_value = "SELECT COUNT(*) FROM orders"
    
    return service


@pytest.fixture(autouse=True)
def reset_services():
    """Reset service singletons before each test"""
    import agents.nodes
    agents.nodes._bigquery_service = None
    agents.nodes._schema_service = None
    agents.nodes._llm_service = None
    yield
    # Cleanup after test
    agents.nodes._bigquery_service = None
    agents.nodes._schema_service = None
    agents.nodes._llm_service = None

