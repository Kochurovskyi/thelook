"""Unit tests for CacheService"""
import pytest
import pandas as pd
from datetime import datetime, timedelta, UTC
from services.cache_service import CacheService


@pytest.mark.unit
class TestCacheService:
    """Test suite for CacheService"""
    
    def test_initialization(self):
        """Test cache service initialization"""
        cache = CacheService(ttl_seconds=60)
        assert cache._ttl_seconds == 60
        assert len(cache._query_cache) == 0
    
    def test_cache_result_and_retrieve(self):
        """Test caching and retrieving results"""
        cache = CacheService(ttl_seconds=3600)
        
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})
        query = "SELECT * FROM orders"
        
        # Cache result
        cache.cache_result(query, df)
        
        # Retrieve cached result
        cached_df = cache.get_cached_result(query)
        
        assert cached_df is not None
        assert len(cached_df) == 3
        assert list(cached_df.columns) == ['col1', 'col2']
    
    def test_cache_expiration(self):
        """Test cache expiration"""
        cache = CacheService(ttl_seconds=1)  # Very short TTL
        
        df = pd.DataFrame({'test': [1]})
        query = "SELECT * FROM test"
        
        cache.cache_result(query, df)
        
        # Should be available immediately
        assert cache.get_cached_result(query) is not None
        
        # Wait for expiration (simulate by manipulating expires_at)
        cache_key = cache._generate_cache_key(query)
        cache._query_cache[cache_key]["expires_at"] = datetime.now(UTC) - timedelta(seconds=1)
        
        # Should be None after expiration
        assert cache.get_cached_result(query) is None
    
    def test_cache_key_generation(self):
        """Test cache key generation"""
        cache = CacheService()
        
        key1 = cache._generate_cache_key("SELECT * FROM orders")
        key2 = cache._generate_cache_key("SELECT * FROM orders")
        key3 = cache._generate_cache_key("SELECT * FROM products")
        
        # Same query should generate same key
        assert key1 == key2
        # Different queries should generate different keys
        assert key1 != key3
    
    def test_cache_key_with_parameters(self):
        """Test cache key generation with parameters"""
        cache = CacheService()
        
        key1 = cache._generate_cache_key("SELECT * FROM orders", limit_rows=10)
        key2 = cache._generate_cache_key("SELECT * FROM orders", limit_rows=20)
        
        # Different parameters should generate different keys
        assert key1 != key2
    
    def test_clear_cache(self):
        """Test clearing cache"""
        cache = CacheService()
        
        df = pd.DataFrame({'test': [1]})
        cache.cache_result("SELECT 1", df)
        cache.cache_result("SELECT 2", df)
        
        assert len(cache._query_cache) == 2
        
        cache.clear_cache()
        
        assert len(cache._query_cache) == 0
    
    def test_get_cache_stats(self):
        """Test cache statistics"""
        cache = CacheService(ttl_seconds=3600)
        
        df = pd.DataFrame({'test': [1]})
        cache.cache_result("SELECT 1", df)
        cache.cache_result("SELECT 2", df)
        
        stats = cache.get_cache_stats()
        
        assert stats["total_entries"] == 2
        assert stats["valid_entries"] == 2
        assert stats["expired_entries"] == 0
        assert stats["ttl_seconds"] == 3600
    
    def test_cleanup_expired(self):
        """Test cleanup of expired entries"""
        cache = CacheService(ttl_seconds=3600)
        
        df = pd.DataFrame({'test': [1]})
        cache.cache_result("SELECT 1", df)
        cache.cache_result("SELECT 2", df)
        
        # Manually expire one entry
        cache_key = cache._generate_cache_key("SELECT 1")
        cache._query_cache[cache_key]["expires_at"] = datetime.now(UTC) - timedelta(seconds=1)
        
        cleaned = cache.cleanup_expired()
        
        assert cleaned == 1
        assert len(cache._query_cache) == 1
    
    def test_cache_with_empty_dataframe(self):
        """Test caching empty DataFrame"""
        cache = CacheService()
        
        empty_df = pd.DataFrame()
        cache.cache_result("SELECT * FROM empty", empty_df)
        
        cached = cache.get_cached_result("SELECT * FROM empty")
        
        assert cached is not None
        assert len(cached) == 0
    
    def test_cache_with_large_dataframe(self):
        """Test caching large DataFrame"""
        cache = CacheService()
        
        large_df = pd.DataFrame({'id': range(10000)})
        cache.cache_result("SELECT * FROM large", large_df)
        
        cached = cache.get_cached_result("SELECT * FROM large")
        
        assert cached is not None
        assert len(cached) == 10000

