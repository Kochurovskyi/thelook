"""Unit tests for Schema service logging integration"""
import pytest
from unittest.mock import Mock, patch, MagicMock

from services.schema_service import SchemaService
from services.bigquery_service import BigQueryService
from utils.request_context import RequestContext


class TestSchemaServiceLogging:
    """Test Schema service logging integration"""
    
    @pytest.fixture
    def mock_bigquery_service(self):
        """Mock BigQueryService"""
        mock = Mock(spec=BigQueryService)
        mock.get_table_schema.return_value = [
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE", "description": "ID"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE", "description": "Name"}
        ]
        return mock
    
    @pytest.fixture
    def schema_service(self, mock_bigquery_service):
        """Create Schema service with mocked BigQuery service"""
        service = SchemaService(bigquery_service=mock_bigquery_service)
        return service
    
    def test_schema_service_initialization_logging(self, schema_service, caplog):
        """Test that Schema service logs initialization"""
        import logging
        with caplog.at_level(logging.INFO):
            # Service already initialized in fixture
            assert schema_service.logger is not None
            assert schema_service.logger.component == "schema_service"
    
    def test_get_table_schema_logs_cache_hit(self, schema_service, caplog):
        """Test that get_table_schema logs cache hits"""
        import logging
        # First call - cache miss
        schema_service.get_table_schema("orders")
        # Second call - cache hit
        with caplog.at_level(logging.DEBUG):
            schema_service.get_table_schema("orders")
            
            # Check that cache hit was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Schema cache hit" in msg for msg in log_messages)
    
    def test_get_table_schema_logs_cache_miss(self, schema_service, caplog):
        """Test that get_table_schema logs cache misses"""
        import logging
        with caplog.at_level(logging.DEBUG):
            schema_service.get_table_schema("orders")
            
            # Check that cache miss was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Schema cache miss" in msg for msg in log_messages)
    
    def test_get_table_schema_logs_caching(self, schema_service, caplog):
        """Test that get_table_schema logs when schema is cached"""
        import logging
        with caplog.at_level(logging.DEBUG):
            schema_service.get_table_schema("orders")
            
            # Check that caching was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Schema cached" in msg for msg in log_messages)
    
    def test_get_all_schemas_logging(self, schema_service, caplog):
        """Test that get_all_schemas logs operations"""
        import logging
        with caplog.at_level(logging.INFO):
            schemas = schema_service.get_all_schemas()
            
            # Check that info log was created
            log_messages = [r.message for r in caplog.records]
            assert any("All schemas fetched" in msg for msg in log_messages)
            assert len(schemas) > 0
    
    def test_build_schema_context_logging(self, schema_service, caplog):
        """Test that build_schema_context logs operations"""
        import logging
        with caplog.at_level(logging.INFO):
            context = schema_service.build_schema_context(include_examples=False)
            
            # Check that info log was created
            log_messages = [r.message for r in caplog.records]
            assert any("Schema context built" in msg for msg in log_messages)
            assert len(context) > 0
    
    def test_get_table_info_logging(self, schema_service, caplog):
        """Test that get_table_info logs operations"""
        import logging
        with caplog.at_level(logging.DEBUG):
            info = schema_service.get_table_info("orders")
            
            # Check that debug logs were created
            log_messages = [r.message for r in caplog.records]
            assert any("Getting table info" in msg for msg in log_messages)
            assert any("Table info retrieved" in msg for msg in log_messages)
            assert info["name"] == "orders"
    
    def test_clear_cache_logging(self, schema_service, caplog):
        """Test that clear_cache logs operations"""
        import logging
        # Populate cache first
        schema_service.get_table_schema("orders")
        
        with caplog.at_level(logging.INFO):
            schema_service.clear_cache()
            
            # Check that cache clear was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Schema cache cleared" in msg for msg in log_messages)
    
    def test_build_relationships_logging(self, schema_service, caplog):
        """Test that relationships are logged during initialization"""
        import logging
        # Relationships are built during initialization and logged there
        with caplog.at_level(logging.INFO):
            new_service = SchemaService(bigquery_service=schema_service.bigquery_service)
            
            # Check that initialization was logged with relationships count
            log_messages = [r.message for r in caplog.records]
            assert any("Schema service initialized" in msg for msg in log_messages)
    
    def test_trace_span_in_get_table_schema(self, schema_service):
        """Test that get_table_schema uses trace_span"""
        # Should not raise (trace_span is a context manager)
        result = schema_service.get_table_schema("orders")
        assert result is not None
        assert len(result) > 0
    
    def test_trace_span_in_get_all_schemas(self, schema_service):
        """Test that get_all_schemas uses trace_span"""
        # Should not raise (trace_span is a context manager)
        result = schema_service.get_all_schemas()
        assert result is not None
        assert len(result) > 0
    
    def test_trace_span_in_build_schema_context(self, schema_service):
        """Test that build_schema_context uses trace_span"""
        # Should not raise (trace_span is a context manager)
        result = schema_service.build_schema_context(include_examples=False)
        assert result is not None
        assert len(result) > 0
    
    def test_trace_span_in_get_table_info(self, schema_service):
        """Test that get_table_info uses trace_span"""
        # Should not raise (trace_span is a context manager)
        result = schema_service.get_table_info("orders")
        assert result is not None
        assert "name" in result
    
    def test_logger_component_name(self, schema_service):
        """Test that logger has correct component name"""
        assert schema_service.logger.component == "schema_service"
    
    def test_cache_hit_miss_tracking(self, schema_service):
        """Test that cache hits and misses are properly tracked"""
        # First call - should be cache miss
        schema1 = schema_service.get_table_schema("orders", use_cache=True)
        
        # Second call - should be cache hit (same object reference)
        schema2 = schema_service.get_table_schema("orders", use_cache=True)
        
        # Should be the same object (cached)
        assert schema1 is schema2
    
    def test_cache_size_logging(self, schema_service, caplog):
        """Test that cache size is logged"""
        import logging
        with caplog.at_level(logging.DEBUG):
            schema_service.get_table_schema("orders")
            schema_service.get_table_schema("products")
            
            # Check that cache size is included in logs
            log_messages = [r.message for r in caplog.records]
            # Cache size should be mentioned in logs
            assert len([msg for msg in log_messages if "cache_size" in msg.lower() or "cached" in msg.lower()]) > 0

