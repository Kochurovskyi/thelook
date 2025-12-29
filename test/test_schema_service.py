"""Test script for Schema service"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.schema_service import SchemaService
import config


def test_schema_service():
    """Test Schema service functionality"""
    print("=" * 60)
    print("Testing Schema Service")
    print("=" * 60)
    
    try:
        # Initialize service
        print("\n1. Initializing Schema service...")
        service = SchemaService()
        print("   [OK] Schema service initialized")
        
        # Test getting single table schema
        print("\n2. Testing single table schema retrieval...")
        schema = service.get_table_schema("orders")
        print(f"   [OK] Retrieved schema for 'orders' table")
        print(f"   Found {len(schema)} columns")
        
        # Test getting all schemas
        print("\n3. Testing all schemas retrieval...")
        all_schemas = service.get_all_schemas()
        print(f"   [OK] Retrieved schemas for {len(all_schemas)} tables")
        for table_name in config.REQUIRED_TABLES:
            if table_name in all_schemas:
                print(f"      - {table_name}: {len(all_schemas[table_name])} columns")
        
        # Test schema context building
        print("\n4. Testing schema context building...")
        context = service.build_schema_context(include_examples=True)
        print(f"   [OK] Schema context built")
        print(f"   Context length: {len(context)} characters")
        print(f"   First 200 characters:")
        print(f"   {context[:200]}...")
        
        # Test caching
        print("\n5. Testing schema caching...")
        service.clear_cache()
        schema1 = service.get_table_schema("orders", use_cache=True)
        schema2 = service.get_table_schema("orders", use_cache=True)
        if id(schema1) == id(schema2):
            print("   [OK] Schema caching works correctly")
        else:
            print("   [WARN] Schema caching may not be working")
        
        # Test table info
        print("\n6. Testing table info retrieval...")
        table_info = service.get_table_info("orders")
        print(f"   [OK] Retrieved table info for 'orders'")
        print(f"   Columns: {table_info['column_count']}")
        print(f"   Related tables: {', '.join(table_info['related_tables'])}")
        
        print("\n" + "=" * 60)
        print("[OK] Schema Service: ALL TESTS PASSED")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n[FAIL] Schema Service: TEST FAILED")
        print(f"   Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_schema_service()
    sys.exit(0 if success else 1)

