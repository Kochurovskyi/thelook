"""Caching service for query results and schema information"""
import hashlib
import json
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, UTC
import pandas as pd
from utils.logger import ComponentLogger
from utils.tracing import trace_span


class CacheService:
    """Service for caching query results and other data"""
    
    def __init__(self, ttl_seconds: int = 3600):
        """
        Initialize cache service
        
        Args:
            ttl_seconds: Time-to-live for cached items in seconds (default: 1 hour)
        """
        self.logger = ComponentLogger("cache_service")
        self._query_cache: Dict[str, Dict[str, Any]] = {}
        self._ttl_seconds = ttl_seconds
        
        self.logger.info(
            "Cache service initialized",
            ttl_seconds=ttl_seconds,
            cache_size=len(self._query_cache)
        )
    
    def _generate_cache_key(self, query: str, **kwargs) -> str:
        """
        Generate a cache key from query string and parameters
        
        Args:
            query: SQL query string
            **kwargs: Additional parameters to include in key
            
        Returns:
            MD5 hash of the query and parameters
        """
        # Normalize query (remove extra whitespace, lowercase)
        normalized_query = " ".join(query.upper().split())
        cache_data = {"query": normalized_query, **kwargs}
        cache_string = json.dumps(cache_data, sort_keys=True)
        return hashlib.md5(cache_string.encode()).hexdigest()
    
    def get_cached_result(self, query: str, **kwargs) -> Optional[pd.DataFrame]:
        """
        Get cached query result if available and not expired
        
        Args:
            query: SQL query string
            **kwargs: Additional parameters used in cache key
            
        Returns:
            Cached DataFrame or None if not found/expired
        """
        cache_key = self._generate_cache_key(query, **kwargs)
        cache_key_short = cache_key[:8]
        
        with trace_span("get_cached_result", component="cache_service", cache_key=cache_key_short) as span:
            if cache_key not in self._query_cache:
                self.logger.debug(
                    "Cache miss",
                    cache_key=cache_key_short,
                    cache_size=len(self._query_cache)
                )
                span.set_tag("cache_hit", False)
                return None
            
            cached_item = self._query_cache[cache_key]
            
            # Check if expired
            now = datetime.now(UTC)
            if now > cached_item["expires_at"]:
                del self._query_cache[cache_key]
                self.logger.debug(
                    "Cache entry expired and removed",
                    cache_key=cache_key_short,
                    expired_at=cached_item["expires_at"].isoformat(),
                    cache_size=len(self._query_cache)
                )
                span.set_tag("cache_hit", False)
                span.set_tag("expired", True)
                return None
            
            # Cache hit
            result = cached_item["result"].copy()
            age_seconds = (now - cached_item["cached_at"]).total_seconds()
            
            self.logger.debug(
                "Cache hit",
                cache_key=cache_key_short,
                rows=len(result),
                age_seconds=age_seconds,
                expires_in_seconds=(cached_item["expires_at"] - now).total_seconds(),
                cache_size=len(self._query_cache)
            )
            
            span.set_tag("cache_hit", True)
            span.set_tag("rows", len(result))
            span.set_tag("age_seconds", age_seconds)
            
            return result
    
    def cache_result(self, query: str, result: pd.DataFrame, **kwargs):
        """
        Cache a query result
        
        Args:
            query: SQL query string
            result: DataFrame with query results
            **kwargs: Additional parameters to include in cache key
        """
        cache_key = self._generate_cache_key(query, **kwargs)
        cache_key_short = cache_key[:8]
        
        with trace_span("cache_result", component="cache_service", cache_key=cache_key_short) as span:
            now = datetime.now(UTC)
            expires_at = now + timedelta(seconds=self._ttl_seconds)
            
            self._query_cache[cache_key] = {
                "result": result.copy(),
                "expires_at": expires_at,
                "cached_at": now,
                "query": query
            }
            
            self.logger.debug(
                "Result cached",
                cache_key=cache_key_short,
                rows=len(result),
                columns=len(result.columns),
                ttl_seconds=self._ttl_seconds,
                expires_at=expires_at.isoformat(),
                cache_size=len(self._query_cache)
            )
            
            span.set_tag("rows", len(result))
            span.set_tag("columns", len(result.columns))
            span.set_tag("ttl_seconds", self._ttl_seconds)
            span.set_tag("cache_size", len(self._query_cache))
    
    def clear_cache(self):
        """Clear all cached results"""
        cache_size_before = len(self._query_cache)
        
        with trace_span("clear_cache", component="cache_service") as span:
            self._query_cache.clear()
            
            self.logger.info(
                "Cache cleared",
                entries_cleared=cache_size_before,
                cache_size=len(self._query_cache)
            )
            
            span.set_tag("entries_cleared", cache_size_before)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics
        
        Returns:
            Dictionary with cache statistics
        """
        with trace_span("get_cache_stats", component="cache_service") as span:
            now = datetime.now(UTC)
            valid_entries = sum(
                1 for item in self._query_cache.values()
                if item["expires_at"] > now
            )
            expired_entries = len(self._query_cache) - valid_entries
            
            stats = {
                "total_entries": len(self._query_cache),
                "valid_entries": valid_entries,
                "expired_entries": expired_entries,
                "ttl_seconds": self._ttl_seconds
            }
            
            self.logger.debug(
                "Cache statistics retrieved",
                **stats
            )
            
            span.set_tag("total_entries", stats["total_entries"])
            span.set_tag("valid_entries", stats["valid_entries"])
            span.set_tag("expired_entries", stats["expired_entries"])
            
            return stats
    
    def cleanup_expired(self):
        """Remove expired entries from cache"""
        with trace_span("cleanup_expired", component="cache_service") as span:
            now = datetime.now(UTC)
            cache_size_before = len(self._query_cache)
            
            expired_keys = [
                key for key, item in self._query_cache.items()
                if item["expires_at"] <= now
            ]
            
            for key in expired_keys:
                del self._query_cache[key]
            
            expired_count = len(expired_keys)
            
            if expired_count > 0:
                self.logger.info(
                    "Expired cache entries cleaned up",
                    expired_count=expired_count,
                    cache_size_before=cache_size_before,
                    cache_size_after=len(self._query_cache)
                )
            else:
                self.logger.debug(
                    "No expired entries to clean up",
                    cache_size=len(self._query_cache)
                )
            
            span.set_tag("expired_count", expired_count)
            span.set_tag("cache_size_before", cache_size_before)
            span.set_tag("cache_size_after", len(self._query_cache))
            
            return expired_count

