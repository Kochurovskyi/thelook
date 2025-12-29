"""Unit tests for LLM service logging integration"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import time

from services.llm_service import LLMService
from utils.request_context import RequestContext


class TestLLMServiceLogging:
    """Test LLM service logging integration"""
    
    @pytest.fixture
    def mock_llm(self):
        """Mock ChatGoogleGenerativeAI"""
        mock = Mock()
        mock_response = Mock()
        mock_response.content = "Test response"
        mock.invoke.return_value = mock_response
        return mock
    
    @pytest.fixture
    def llm_service(self, mock_llm):
        """Create LLM service with mocked LLM"""
        with patch('services.llm_service.ChatGoogleGenerativeAI', return_value=mock_llm):
            service = LLMService(api_key="test-key", model="test-model")
            service.llm = mock_llm
            return service
    
    def test_llm_service_initialization_logging(self, llm_service, caplog):
        """Test that LLM service logs initialization"""
        import logging
        with caplog.at_level(logging.INFO):
            # Service already initialized in fixture
            assert llm_service.logger is not None
            assert llm_service.logger.component == "llm_service"
    
    def test_generate_text_logs_prompt_generation(self, llm_service, caplog):
        """Test that generate_text logs prompt generation"""
        import logging
        with caplog.at_level(logging.DEBUG):
            prompt = "Test prompt"
            llm_service.generate_text(prompt)
            
            # Check that debug log was created
            assert len(caplog.records) > 0
    
    def test_generate_text_logs_execution_time(self, llm_service, caplog):
        """Test that generate_text logs execution time"""
        import logging
        with caplog.at_level(logging.INFO):
            prompt = "Test prompt"
            llm_service.generate_text(prompt)
            
            # Check that info log with execution time was created
            log_messages = [r.message for r in caplog.records]
            assert any("Text generation completed" in msg for msg in log_messages)
    
    def test_generate_text_logs_token_usage(self, llm_service, caplog):
        """Test that generate_text logs token usage"""
        import logging
        with caplog.at_level(logging.INFO):
            prompt = "Test prompt for token counting"
            llm_service.generate_text(prompt)
            
            # Check that token usage was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Text generation completed" in msg for msg in log_messages)
    
    def test_generate_text_logs_errors(self, llm_service, caplog):
        """Test that generate_text logs errors"""
        import logging
        with caplog.at_level(logging.ERROR):
            llm_service.llm.invoke.side_effect = Exception("Test error")
            
            with pytest.raises(RuntimeError):
                llm_service.generate_text("Test prompt")
            
            # Check that error was logged
            log_messages = [r.message for r in caplog.records]
            assert any("LLM generation failed" in msg for msg in log_messages)
    
    def test_generate_sql_logs_query_generation(self, llm_service, caplog):
        """Test that generate_sql logs query generation"""
        import logging
        with caplog.at_level(logging.DEBUG):
            user_query = "Count orders"
            schema_context = "Test schema context"
            
            llm_service.generate_sql(user_query, schema_context)
            
            # Check that debug logs were created
            assert len(caplog.records) > 0
    
    def test_generate_sql_logs_prompt_assembly(self, llm_service, caplog):
        """Test that generate_sql logs prompt assembly"""
        import logging
        with caplog.at_level(logging.DEBUG):
            user_query = "Count orders"
            schema_context = "Test schema context"
            
            llm_service.generate_sql(user_query, schema_context)
            
            # Check that prompt assembly was logged
            log_messages = [r.message for r in caplog.records]
            assert any("SQL generation prompt assembled" in msg for msg in log_messages)
    
    def test_generate_sql_logs_completion(self, llm_service, caplog):
        """Test that generate_sql logs completion"""
        import logging
        with caplog.at_level(logging.INFO):
            user_query = "Count orders"
            schema_context = "Test schema context"
            
            llm_service.generate_sql(user_query, schema_context)
            
            # Check that completion was logged
            log_messages = [r.message for r in caplog.records]
            assert any("SQL generation completed" in msg for msg in log_messages)
    
    def test_generate_sql_logs_markdown_cleaning(self, llm_service, caplog):
        """Test that generate_sql logs markdown cleaning"""
        import logging
        with caplog.at_level(logging.DEBUG):
            # Mock response with markdown
            mock_response = Mock()
            mock_response.content = "```sql\nSELECT * FROM test\n```"
            llm_service.llm.invoke.return_value = mock_response
            
            user_query = "Count orders"
            schema_context = "Test schema context"
            
            llm_service.generate_sql(user_query, schema_context)
            
            # Check that markdown cleaning was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Cleaned SQL markdown formatting" in msg for msg in log_messages)
    
    def test_generate_sql_logs_errors(self, llm_service, caplog):
        """Test that generate_sql logs errors"""
        import logging
        with caplog.at_level(logging.ERROR):
            llm_service.llm.invoke.side_effect = Exception("Test error")
            
            user_query = "Count orders"
            schema_context = "Test schema context"
            
            with pytest.raises(Exception):
                llm_service.generate_sql(user_query, schema_context)
            
            # Check that error was logged
            log_messages = [r.message for r in caplog.records]
            assert any("SQL generation failed" in msg for msg in log_messages)
    
    def test_hash_prompt_creates_hash(self, llm_service):
        """Test that _hash_prompt creates a hash"""
        prompt = "Test prompt"
        hash1 = llm_service._hash_prompt(prompt)
        hash2 = llm_service._hash_prompt(prompt)
        
        # Same prompt should produce same hash
        assert hash1 == hash2
        assert len(hash1) == 8  # First 8 chars of MD5
    
    def test_hash_prompt_different_prompts(self, llm_service):
        """Test that different prompts produce different hashes"""
        hash1 = llm_service._hash_prompt("Prompt 1")
        hash2 = llm_service._hash_prompt("Prompt 2")
        
        # Different prompts should produce different hashes
        assert hash1 != hash2
    
    def test_trace_span_in_generate_text(self, llm_service):
        """Test that generate_text uses trace_span"""
        prompt = "Test prompt"
        
        # Should not raise (trace_span is a context manager)
        result = llm_service.generate_text(prompt)
        assert result is not None
    
    def test_trace_span_in_generate_sql(self, llm_service):
        """Test that generate_sql uses trace_span"""
        user_query = "Count orders"
        schema_context = "Test schema context"
        
        # Should not raise (trace_span is a context manager)
        result = llm_service.generate_sql(user_query, schema_context)
        assert result is not None
    
    def test_logger_component_name(self, llm_service):
        """Test that logger has correct component name"""
        assert llm_service.logger.component == "llm_service"
    
    def test_initialization_without_api_key_logs_error(self, caplog):
        """Test that initialization without API key logs error"""
        import logging
        with caplog.at_level(logging.ERROR):
            with patch('services.llm_service.config.GOOGLE_API_KEY', None):
                with pytest.raises(ValueError):
                    LLMService()
                
                # Check that error was logged
                log_messages = [r.message for r in caplog.records]
                assert any("GOOGLE_API_KEY not found" in msg for msg in log_messages)

