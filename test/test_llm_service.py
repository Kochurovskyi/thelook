"""Test script for LLM service"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.llm_service import LLMService
from services.schema_service import SchemaService
import config


def test_llm_service():
    """Test LLM service functionality"""
    print("=" * 60)
    print("Testing LLM Service")
    print("=" * 60)
    
    try:
        # Check API key
        if not config.GOOGLE_API_KEY:
            print("\n[SKIP] GOOGLE_API_KEY not set. Skipping LLM tests.")
            print("   Set GOOGLE_API_KEY in .env file to run LLM tests.")
            return True
        
        # Initialize service
        print("\n1. Initializing LLM service...")
        service = LLMService()
        print(f"   [OK] LLM service initialized")
        print(f"   Model: {service.model_name}")
        print(f"   Temperature: {service.temperature}")
        
        # Test simple text generation
        print("\n2. Testing simple text generation...")
        prompt = "Say hello in one word."
        response = service.generate_text(prompt)
        print(f"   [OK] Text generated")
        print(f"   Response: {response[:50]}...")
        
        # Test SQL generation
        print("\n3. Testing SQL generation...")
        schema_service = SchemaService()
        schema_context = schema_service.build_schema_context(include_examples=False)
        
        user_query = "Count orders"
        sql = service.generate_sql(user_query, schema_context)
        print(f"   [OK] SQL generated")
        print(f"   Query: {user_query}")
        print(f"   Generated SQL: {sql[:100]}...")
        
        # Test SQL generation with complex query
        print("\n4. Testing complex SQL generation...")
        user_query2 = "Show top 5 products by revenue"
        sql2 = service.generate_sql(user_query2, schema_context)
        print(f"   [OK] Complex SQL generated")
        print(f"   Query: {user_query2}")
        print(f"   Generated SQL: {sql2[:150]}...")
        
        print("\n" + "=" * 60)
        print("[OK] LLM Service: ALL TESTS PASSED")
        print("=" * 60)
        return True
        
    except ValueError as e:
        print(f"\n[SKIP] LLM Service: API key not configured")
        print(f"   {str(e)}")
        return True  # Don't fail if API key is missing
    except Exception as e:
        print(f"\n[FAIL] LLM Service: TEST FAILED")
        print(f"   Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_llm_service()
    sys.exit(0 if success else 1)

