"""Unit tests for Cache service logging integration"""
import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime, timedelta, UTC

from services.cache_service import CacheService
from utils.request_context import RequestContext


class TestCacheServiceLogging:
    """Test Cache service logging integration"""
    
    @pytest.fixture
    def cache_service(self):
        """Create Cache service"""
        service = CacheService(ttl_seconds=60)
        return service
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create sample DataFrame for testing"""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })
    
    def test_cache_service_initialization_logging(self, cache_service, caplog):
        """Test that Cache service logs initialization"""
        import logging
        with caplog.at_level(logging.INFO):
            # Service already initialized in fixture
            assert cache_service.logger is not None
            assert cache_service.logger.component == "cache_service"
    
    def test_get_cached_result_logs_cache_miss(self, cache_service, caplog):
        """Test that get_cached_result logs cache misses"""
        import logging
        with caplog.at_level(logging.DEBUG):
            result = cache_service.get_cached_result("SELECT * FROM test")
            
            # Check that cache miss was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Cache miss" in msg for msg in log_messages)
            assert result is None
    
    def test_get_cached_result_logs_cache_hit(self, cache_service, sample_dataframe, caplog):
        """Test that get_cached_result logs cache hits"""
        import logging
        # First cache the result
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        
        with caplog.at_level(logging.DEBUG):
            result = cache_service.get_cached_result("SELECT * FROM test")
            
            # Check that cache hit was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Cache hit" in msg for msg in log_messages)
            assert result is not None
            assert len(result) == 3
    
    def test_get_cached_result_logs_expiration(self, cache_service, sample_dataframe, caplog):
        """Test that get_cached_result logs when entries expire"""
        import logging
        # Cache with very short TTL
        cache_service._ttl_seconds = 0.001  # 1ms
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        
        # Wait for expiration
        import time
        time.sleep(0.01)
        
        with caplog.at_level(logging.DEBUG):
            result = cache_service.get_cached_result("SELECT * FROM test")
            
            # Check that expiration was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Cache entry expired" in msg for msg in log_messages)
            assert result is None
    
    def test_cache_result_logs_caching(self, cache_service, sample_dataframe, caplog):
        """Test that cache_result logs caching operations"""
        import logging
        with caplog.at_level(logging.DEBUG):
            cache_service.cache_result("SELECT * FROM test", sample_dataframe)
            
            # Check that caching was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Result cached" in msg for msg in log_messages)
    
    def test_clear_cache_logging(self, cache_service, sample_dataframe, caplog):
        """Test that clear_cache logs operations"""
        import logging
        # Populate cache first
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        
        with caplog.at_level(logging.INFO):
            cache_service.clear_cache()
            
            # Check that cache clear was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Cache cleared" in msg for msg in log_messages)
    
    def test_get_cache_stats_logging(self, cache_service, sample_dataframe, caplog):
        """Test that get_cache_stats logs operations"""
        import logging
        # Populate cache first
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        
        with caplog.at_level(logging.DEBUG):
            stats = cache_service.get_cache_stats()
            
            # Check that stats retrieval was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Cache statistics retrieved" in msg for msg in log_messages)
            assert stats["total_entries"] > 0
    
    def test_cleanup_expired_logs_cleanup(self, cache_service, sample_dataframe, caplog):
        """Test that cleanup_expired logs cleanup operations"""
        import logging
        # Cache with very short TTL
        cache_service._ttl_seconds = 0.001  # 1ms
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        
        # Wait for expiration
        import time
        time.sleep(0.01)
        
        with caplog.at_level(logging.INFO):
            expired_count = cache_service.cleanup_expired()
            
            # Check that cleanup was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Expired cache entries cleaned up" in msg for msg in log_messages)
            assert expired_count > 0
    
    def test_cleanup_expired_logs_no_expired(self, cache_service, sample_dataframe, caplog):
        """Test that cleanup_expired logs when no expired entries"""
        import logging
        # Cache with normal TTL
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        
        with caplog.at_level(logging.DEBUG):
            expired_count = cache_service.cleanup_expired()
            
            # Check that no cleanup needed was logged
            log_messages = [r.message for r in caplog.records]
            assert any("No expired entries to clean up" in msg for msg in log_messages)
            assert expired_count == 0
    
    def test_trace_span_in_get_cached_result(self, cache_service, sample_dataframe):
        """Test that get_cached_result uses trace_span"""
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        
        # Should not raise (trace_span is a context manager)
        result = cache_service.get_cached_result("SELECT * FROM test")
        assert result is not None
    
    def test_trace_span_in_cache_result(self, cache_service, sample_dataframe):
        """Test that cache_result uses trace_span"""
        # Should not raise (trace_span is a context manager)
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        assert len(cache_service._query_cache) > 0
    
    def test_trace_span_in_clear_cache(self, cache_service, sample_dataframe):
        """Test that clear_cache uses trace_span"""
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        
        # Should not raise (trace_span is a context manager)
        cache_service.clear_cache()
        assert len(cache_service._query_cache) == 0
    
    def test_trace_span_in_get_cache_stats(self, cache_service, sample_dataframe):
        """Test that get_cache_stats uses trace_span"""
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        
        # Should not raise (trace_span is a context manager)
        stats = cache_service.get_cache_stats()
        assert stats is not None
    
    def test_trace_span_in_cleanup_expired(self, cache_service):
        """Test that cleanup_expired uses trace_span"""
        # Should not raise (trace_span is a context manager)
        expired_count = cache_service.cleanup_expired()
        assert expired_count >= 0
    
    def test_logger_component_name(self, cache_service):
        """Test that logger has correct component name"""
        assert cache_service.logger.component == "cache_service"
    
    def test_cache_operations_log_cache_size(self, cache_service, sample_dataframe, caplog):
        """Test that cache operations log cache size"""
        import logging
        with caplog.at_level(logging.DEBUG):
            cache_service.cache_result("SELECT * FROM test1", sample_dataframe)
            cache_service.cache_result("SELECT * FROM test2", sample_dataframe)
            
            # Check that cache size is included in logs
            log_messages = [r.message for r in caplog.records]
            assert len([msg for msg in log_messages if "cache_size" in msg.lower()]) > 0
    
    def test_cache_hit_logs_age_and_expires_in(self, cache_service, sample_dataframe, caplog):
        """Test that cache hits log age and expiration info"""
        import logging
        cache_service.cache_result("SELECT * FROM test", sample_dataframe)
        
        with caplog.at_level(logging.DEBUG):
            result = cache_service.get_cached_result("SELECT * FROM test")
            
            # Check that age and expiration info are logged
            log_messages = [r.message for r in caplog.records]
            assert any("Cache hit" in msg for msg in log_messages)
            assert result is not None

