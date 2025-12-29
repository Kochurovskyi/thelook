"""Test script for BigQuery service"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.bigquery_service import BigQueryService
import config


def test_bigquery_service():
    """Test BigQuery service functionality"""
    print("=" * 60)
    print("Testing BigQuery Service")
    print("=" * 60)
    
    try:
        # Initialize service
        print("\n1. Initializing BigQuery service...")
        service = BigQueryService()
        print(f"   [OK] Client initialized (Project: {service.project_id or 'default'})")
        
        # Test connection
        print("\n2. Testing connection...")
        if service.test_connection():
            print("   [OK] Connection successful")
        else:
            print("   [FAIL] Connection failed")
            return False
        
        # Test simple query
        print("\n3. Testing simple COUNT query...")
        query = f"SELECT COUNT(*) as row_count FROM `{config.DATASET_ID}.orders`"
        result = service.execute_query(query)
        print(f"   [OK] Query executed successfully")
        print(f"   Result: {result.iloc[0]['row_count']:,} rows in orders table")
        
        # Test schema introspection
        print("\n4. Testing schema introspection...")
        schema = service.get_table_schema("orders")
        print(f"   [OK] Schema retrieved for 'orders' table")
        print(f"   Found {len(schema)} columns:")
        for col in schema[:5]:  # Show first 5 columns
            print(f"      - {col['name']} ({col['type']})")
        if len(schema) > 5:
            print(f"      ... and {len(schema) - 5} more columns")
        
        # Test error handling
        print("\n5. Testing error handling...")
        try:
            service.execute_query("SELECT * FROM non_existent_table")
            print("   [FAIL] Should have raised an error")
            return False
        except RuntimeError:
            print("   [OK] Invalid query handled gracefully")
        
        print("\n" + "=" * 60)
        print("[OK] BigQuery Service: ALL TESTS PASSED")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n[FAIL] BigQuery Service: TEST FAILED")
        print(f"   Error: {str(e)}")
        return False


if __name__ == "__main__":
    success = test_bigquery_service()
    sys.exit(0 if success else 1)

