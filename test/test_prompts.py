"""Test script for Prompts"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prompts.sql_generation import get_sql_generation_prompt, get_few_shot_examples
from services.schema_service import SchemaService


def test_prompts():
    """Test Prompt functionality"""
    print("=" * 60)
    print("Testing SQL Generation Prompts")
    print("=" * 60)
    
    try:
        # Test few-shot examples
        print("\n1. Testing few-shot examples...")
        examples = get_few_shot_examples()
        print(f"   [OK] Few-shot examples retrieved")
        print(f"   Examples length: {len(examples)} characters")
        
        # Test prompt generation
        print("\n2. Testing prompt generation...")
        schema_service = SchemaService()
        schema_context = schema_service.build_schema_context(include_examples=False)
        
        user_query = "Count orders"
        prompt = get_sql_generation_prompt(
            user_query=user_query,
            schema_context=schema_context,
            few_shot_examples=get_few_shot_examples()
        )
        
        print(f"   [OK] Prompt generated")
        print(f"   Prompt length: {len(prompt)} characters")
        print(f"   Contains schema: {'DATABASE SCHEMA' in prompt}")
        print(f"   Contains user query: {user_query in prompt}")
        print(f"   Contains safety rules: {'SAFETY RULES' in prompt}")
        print(f"   Contains examples: {'EXAMPLES' in prompt}")
        
        # Test prompt without examples
        print("\n3. Testing prompt without examples...")
        prompt_no_examples = get_sql_generation_prompt(
            user_query=user_query,
            schema_context=schema_context,
            few_shot_examples=None
        )
        print(f"   [OK] Prompt without examples generated")
        print(f"   Length difference: {len(prompt) - len(prompt_no_examples)} characters")
        
        print("\n" + "=" * 60)
        print("[OK] Prompts: ALL TESTS PASSED")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n[FAIL] Prompts: TEST FAILED")
        print(f"   Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_prompts()
    sys.exit(0 if success else 1)

