import os
from dotenv import load_dotenv
load_dotenv()

# Magento API Credentials
M2_BASE_URL = os.getenv("M2_BASE_URL")              # "https://your-magento-store.com"
M2_ACCESS_TOKEN = os.getenv("M2_ACCESS_TOKEN")      # Generate from Magento Admin
M2_USERNAME = os.getenv("M2_USERNAME")              # Your Magento Username (as shown in 1Pswd)
M2_PASSWORD = os.getenv("M2_PASSWORD")              # Your Magento Password (as shown in 1Pswd)

# BigQuery Credentials and Table Information
BQ_PATH_KEY = os.getenv("BQ_PATH_KEY")              # "/path/to/your/service-account-key.json" generated from BQ. Should be in the same directory as this script
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")          # "project-id"
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", )        # "dataset-id"

BQ_ORDER_TABLE_ID = "orders"                        # "table-id"
BQ_CUSTOMER_TABLE_ID = "customers"                  # "table-id"

# Date Range for Data Fetching
FROM_DATE = "2025-01-02"
TO_DATE = "2025-01-03"

# Reset BQ Tables (True to reset data in BigQuery, False if incremental load)
RESET = "False"                                     # Keep it as a string, not a boolean


