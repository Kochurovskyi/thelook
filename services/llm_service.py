"""LLM service for Gemini integration"""
from typing import Optional
from langchain_google_genai import ChatGoogleGenerativeAI
import time
import config
from utils.logger import ComponentLogger
from utils.tracing import trace_span
from utils.request_context import RequestContext


class LLMService:
    """Service for interacting with Gemini LLM"""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        """
        Initialize LLM service with Gemini
        
        Args:
            api_key: Optional API key. Uses config if None.
            model: Optional model name. Uses config if None.
        """
        self.logger = ComponentLogger("llm_service")
        self.api_key = api_key or config.GOOGLE_API_KEY
        self.model_name = model or config.LLM_MODEL
        self.temperature = config.LLM_TEMPERATURE
        
        if not self.api_key:
            self.logger.error("GOOGLE_API_KEY not found")
            raise ValueError(
                "GOOGLE_API_KEY not found. "
                "Please set it in .env file or environment variable."
            )
        
        self.llm = ChatGoogleGenerativeAI(
            model=self.model_name,
            google_api_key=self.api_key,
            temperature=self.temperature
        )
        
        self.logger.info(
            "LLM service initialized",
            model=self.model_name,
            temperature=self.temperature
        )
    
    def generate_text(self, prompt: str) -> str:
        """
        Generate text from prompt
        
        Args:
            prompt: Input prompt
            
        Returns:
            Generated text response
        """
        prompt_hash = self._hash_prompt(prompt)
        with trace_span("generate_text", component="llm_service", prompt_hash=prompt_hash) as span:
            try:
                # Log prompt generation (sanitized - no sensitive data)
                self.logger.debug(
                    "Generating text from prompt",
                    prompt_hash=prompt_hash,
                    prompt_length=len(prompt),
                    model=self.model_name
                )
                
                start_time = time.time()
                response = self.llm.invoke(prompt)
                end_time = time.time()
                execution_time_ms = (end_time - start_time) * 1000
                
                # Estimate token usage (rough approximation: 1 token ≈ 4 characters)
                input_tokens = len(prompt) // 4
                output_tokens = len(response.content) // 4
                total_tokens = input_tokens + output_tokens
                
                # Log successful generation
                self.logger.info(
                    "Text generation completed",
                    prompt_hash=prompt_hash,
                    execution_time_ms=execution_time_ms,
                    input_tokens_approx=input_tokens,
                    output_tokens_approx=output_tokens,
                    total_tokens_approx=total_tokens,
                    response_length=len(response.content),
                    model=self.model_name
                )
                
                span.set_tag("execution_time_ms", execution_time_ms)
                span.set_tag("total_tokens_approx", total_tokens)
                span.set_tag("success", True)
                
                return response.content
                
            except Exception as e:
                error_type = type(e).__name__
                self.logger.error(
                    "LLM generation failed",
                    prompt_hash=prompt_hash,
                    error_type=error_type,
                    error_message=str(e),
                    model=self.model_name
                )
                span.set_tag("error", True)
                span.set_tag("error_type", error_type)
                span.log(f"LLM generation failed: {str(e)}", level="ERROR")
                raise RuntimeError(f"LLM generation failed: {str(e)}")
    
    def _hash_prompt(self, prompt: str) -> str:
        """Create a hash of the prompt for logging (without exposing content)"""
        import hashlib
        return hashlib.md5(prompt.encode('utf-8')).hexdigest()[:8]
    
    def generate_sql(self, user_query: str, schema_context: str) -> str:
        """
        Generate SQL query from natural language query
        
        Args:
            user_query: User's natural language question
            schema_context: Database schema context
            
        Returns:
            Generated SQL query string
        """
        from prompts.sql_generation import get_sql_generation_prompt, get_few_shot_examples
        
        query_hash = self._hash_prompt(user_query)
        with trace_span("generate_sql", component="llm_service", query_hash=query_hash) as span:
            try:
                self.logger.debug(
                    "Generating SQL query",
                    query_hash=query_hash,
                    user_query_length=len(user_query),
                    schema_context_length=len(schema_context),
                    model=self.model_name
                )
                
                prompt = get_sql_generation_prompt(
                    user_query=user_query,
                    schema_context=schema_context,
                    few_shot_examples=get_few_shot_examples()
                )
                
                prompt_hash = self._hash_prompt(prompt)
                self.logger.debug(
                    "SQL generation prompt assembled",
                    query_hash=query_hash,
                    prompt_hash=prompt_hash,
                    prompt_length=len(prompt)
                )
                
                sql = self.generate_text(prompt)
                
                # Clean up SQL - remove markdown code blocks if present
                original_sql = sql
                sql = sql.strip()
                if sql.startswith("```sql"):
                    sql = sql[6:]
                elif sql.startswith("```"):
                    sql = sql[3:]
                if sql.endswith("```"):
                    sql = sql[:-3]
                sql = sql.strip()
                
                if original_sql != sql:
                    self.logger.debug(
                        "Cleaned SQL markdown formatting",
                        query_hash=query_hash,
                        original_length=len(original_sql),
                        cleaned_length=len(sql)
                    )
                
                self.logger.info(
                    "SQL generation completed",
                    query_hash=query_hash,
                    sql_length=len(sql),
                    model=self.model_name
                )
                
                span.set_tag("sql_length", len(sql))
                span.set_tag("success", True)
                
                return sql
                
            except Exception as e:
                error_type = type(e).__name__
                self.logger.error(
                    "SQL generation failed",
                    query_hash=query_hash,
                    error_type=error_type,
                    error_message=str(e),
                    model=self.model_name
                )
                span.set_tag("error", True)
                span.set_tag("error_type", error_type)
                raise

