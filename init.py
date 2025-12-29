import os
import sys
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
import subprocess

# Try to get project from gcloud config
try:
    result = subprocess.run(['gcloud', 'config', 'get-value', 'project'], capture_output=True, text=True, timeout=5)
    if result.returncode == 0 and result.stdout.strip():
        project_id = result.stdout.strip()
        os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
except: project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
# Construct a BigQuery client object.
# For public datasets, you still need authentication but can use any project.
try: client = bigquery.Client(project=project_id) if project_id else bigquery.Client()
except DefaultCredentialsError: 
    print("ERROR: Google Cloud credentials not found.")
    sys.exit(1)
dataset_id = "bigquery-public-data.thelook_ecommerce"               # Reference to the thelook_ecommerce dataset
required_tables = ["orders", "order_items", "products", "users"]    # Required tables
print(f"Row counts for tables in {dataset_id}:\n")
for table_name in required_tables:
    query = f"""SELECT COUNT(*) as row_count FROM `{dataset_id}.{table_name}`"""
    rows = client.query_and_wait(query)
    row_count = list(rows)[0]["row_count"]
    print(f"{table_name}: {row_count:,} rows")
