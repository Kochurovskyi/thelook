import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Early check for critical dependencies before validation
def _check_dependencies():
    """Check if critical dependencies are available before proceeding."""
    missing_deps = []
    
    try:
        from google.cloud import bigquery
    except ImportError as e:
        missing_deps.append(f"google-cloud-bigquery: {str(e)}")
    
    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
    except ImportError as e:
        missing_deps.append(f"langchain-google-genai: {str(e)}")
    
    if missing_deps:
        print("ERROR: Critical dependencies are missing or not properly installed.", file=sys.stderr)
        print("", file=sys.stderr)
        print("Missing or broken packages:", file=sys.stderr)
        for dep in missing_deps:
            print(f"  - {dep}", file=sys.stderr)
        print("", file=sys.stderr)
        print("To fix:", file=sys.stderr)
        print("  1. Ensure you're using the correct Python interpreter (virtual environment)", file=sys.stderr)
        print("  2. Activate your virtual environment:", file=sys.stderr)
        print("     Windows: .venv\\Scripts\\activate", file=sys.stderr)
        print("     Linux/Mac: source .venv/bin/activate", file=sys.stderr)
        print("  3. Install dependencies: pip install -r requirements.txt", file=sys.stderr)
        print("  4. Use 'python' instead of 'py' to ensure correct interpreter", file=sys.stderr)
        sys.exit(1)

# Check dependencies before validation
_check_dependencies()

# BigQuery Configuration
DATASET_ID = "bigquery-public-data.thelook_ecommerce"
REQUIRED_TABLES = ["orders", "order_items", "products", "users"]

# Get project ID from environment or gcloud config
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")

# Gemini/LLM Configuration
# Support both GOOGLE_API_KEY and GEMINI_API_KEY for compatibility
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
LLM_MODEL = os.environ.get("LLM_MODEL", "gemini-2.5-flash")  # or gemini-2.5-flash-lite when available
LLM_TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", "0.1"))  # Low temperature for SQL generation

# Agent Configuration
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
GRAPH_LOG_LEVEL = os.environ.get("GRAPH_LOG_LEVEL", "INFO")

# Query Configuration
MAX_QUERY_ROWS = int(os.environ.get("MAX_QUERY_ROWS", "10000"))  # Limit for safety
QUERY_TIMEOUT = int(os.environ.get("QUERY_TIMEOUT", "300"))  # 5 minutes

# Visualization Configuration
DEFAULT_CHART_WIDTH = int(os.environ.get("DEFAULT_CHART_WIDTH", "700"))
DEFAULT_CHART_HEIGHT = int(os.environ.get("DEFAULT_CHART_HEIGHT", "400"))

# Logging Configuration (Phase 5.1)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.environ.get("LOG_FORMAT", "json")  # json or console
LOG_FILE = os.environ.get("LOG_FILE", "logs/app.log")
LOG_DIR = os.environ.get("LOG_DIR", "logs")
ENABLE_REQUEST_TRACING = os.environ.get("ENABLE_REQUEST_TRACING", "true").lower() == "true"
ENABLE_METRICS = os.environ.get("ENABLE_METRICS", "true").lower() == "true"


def validate_google_api_key():
    """
    Validate GOOGLE_API_KEY by making a simple test API call.
    Terminates the application if validation fails.
    """
    if not GOOGLE_API_KEY:
        print("ERROR: GOOGLE_API_KEY is not set.", file=sys.stderr)
        print("Please set GOOGLE_API_KEY or GEMINI_API_KEY in your .env file or environment variables.", file=sys.stderr)
        sys.exit(1)
    
    # Try multiple model names in case the default model name is invalid
    models_to_try = [LLM_MODEL, "gemini-1.5-flash", "gemini-1.5-pro", "gemini-pro"]
    
    for model_name in models_to_try:
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            
            # Create a test LLM instance
            test_llm = ChatGoogleGenerativeAI(
                model=model_name,
                google_api_key=GOOGLE_API_KEY,
                temperature=0.1
            )
            
            # Make a simple test call
            test_response = test_llm.invoke("test")
            
            # Check if we got a valid response
            if test_response and hasattr(test_response, 'content'):
                # If we used a different model than configured, update it
                if model_name != LLM_MODEL:
                    print(f"WARNING: Model '{LLM_MODEL}' not available. Using '{model_name}' instead.", file=sys.stderr)
                    # Update the model in config (this is a workaround - ideally we'd set it properly)
                    os.environ["LLM_MODEL"] = model_name
                return  # Validation successful
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # If it's a model name error, try next model
            if "model" in error_msg and ("not found" in error_msg or "invalid" in error_msg or "not available" in error_msg):
                continue  # Try next model
            
            # If it's an API key error, fail immediately
            if "api key" in error_msg or "authentication" in error_msg or ("invalid" in error_msg and "argument" in error_msg) or "api_key_invalid" in error_msg:
                print("ERROR: GOOGLE_API_KEY validation failed - Invalid API key.", file=sys.stderr)
                print("", file=sys.stderr)
                sys.exit(1)
            
            # For other errors, try next model or fail if last model
            if model_name == models_to_try[-1]:
                print(f"ERROR: GOOGLE_API_KEY validation failed - {str(e)}", file=sys.stderr)
                print("Please check your API key and network connection.", file=sys.stderr)
                sys.exit(1)
    
    # If we get here, all models failed
    print("ERROR: GOOGLE_API_KEY validation failed - could not validate with any model.", file=sys.stderr)
    print("", file=sys.stderr)
    sys.exit(1)


# Validate API key before app runs
# Skip validation if running tests (pytest sets PYTEST_CURRENT_TEST)
if not os.environ.get("PYTEST_CURRENT_TEST"):
    validate_google_api_key()
