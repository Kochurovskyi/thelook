"""Main entry point for Streamlit app and test runner"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def run_tests(test_module: str = "all"):
    """Run tests for specified module"""
    if test_module == "all":
        # Run all tests
        modules = [
            "bigquery",
            "schema",
            "state",
            "prompts",
            "llm",
            "node",
            "graph",
            "e2e"
        ]
        for mod in modules:
            run_tests(mod)
        return
    
    # Import and run specific test
    try:
        if test_module == "bigquery":
            from test.test_bigquery_service import test_bigquery_service
            test_bigquery_service()
        elif test_module == "schema":
            from test.test_schema_service import test_schema_service
            test_schema_service()
        elif test_module == "state":
            from test.test_state import test_state
            test_state()
        elif test_module == "prompts":
            from test.test_prompts import test_prompts
            test_prompts()
        elif test_module == "llm":
            from test.test_llm_service import test_llm_service
            test_llm_service()
        elif test_module == "node":
            node_type = sys.argv[3] if len(sys.argv) > 3 else "all"
            from test.test_nodes import test_nodes
            test_nodes(node_type)
        elif test_module == "graph":
            from test.test_graph import test_graph
            test_graph()
        elif test_module == "e2e":
            from test.test_e2e import test_e2e
            test_e2e()
        else:
            print(f"Unknown test module: {test_module}")
            print("Available modules: bigquery, schema, state, prompts, llm, node, graph, e2e")
    except ImportError as e:
        print(f"Test module not found: {test_module}")
        print(f"Error: {e}")
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_module = sys.argv[2] if len(sys.argv) > 2 else "all"
        run_tests(test_module)
    else:
        # Normal Streamlit app (to be implemented in Phase 3)
        print("Streamlit app will be implemented in Phase 3")
        print("For now, use: python main.py --test <module_name>")

