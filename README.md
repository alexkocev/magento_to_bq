# Magento to BigQuery Data Sync

This script exports customer and order data from Magento to Google BigQuery. It supports incremental updates, allowing you to keep your BigQuery data synchronized with the latest changes in Magento.

## Features

- Fetch customer and order data from Magento using REST API
- Support for 2-Factor Authentication (2FA)
- Incremental data loading to BigQuery (only new or updated records)
- Full data reset option for BigQuery tables
- Date range filtering for data extraction

## Setup

1. **Clone the repository**

2. **Update the config.py file with your actual values:**

```python
# Magento API Credentials
M2_BASE_URL = "https://your-magento-store.com"     # Your Magento store URL
M2_ACCESS_TOKEN = ""                               # Leave empty, will be generated during runtime
M2_USERNAME = "your_username"                      # Your Magento admin username
M2_PASSWORD = "your_password"                      # Your Magento admin password

# BigQuery Credentials and Table Information
BQ_PATH_KEY = "service-account-key.json"           # Path to BigQuery service account key file
BQ_PROJECT_ID = "your-project-id"                  # Google Cloud project ID
BQ_DATASET_ID = "your_dataset"                     # BigQuery dataset ID
BQ_ORDER_TABLE_ID = "orders"                       # Table for order data
BQ_CUSTOMER_TABLE_ID = "customers"                 # Table for customer data

# Date Range for Data Fetching
FROM_DATE = "2025-01-01"                           # Start date (YYYY-MM-DD)
TO_DATE = "2025-01-31"                             # End date (YYYY-MM-DD)

# Reset BQ Tables (True to reset data in BigQuery, False for incremental load)
RESET = "False"                                    # Keep as string, not boolean
```

3. **Set up BigQuery**
   - Create a dataset named "magento" in BigQuery 
   - Create two tables within this dataset: "customers" and "orders"
   - Download your BigQuery service account key
   - Place the JSON key file in the same directory as the script
   - Make sure the path matches the `BQ_PATH_KEY` value in config.py

4. **Install dependencies**
```
pip install -r requirements.txt
```

## Usage

Run the script with:

```
python main.py
```

When prompted, enter the 6-digit OTP code from your Google Authenticator app.

## Configuration Details

- **Date Range**: Set `FROM_DATE` and `TO_DATE` in config.py to specify the data extraction period
- **Incremental Updates**: By default (`RESET = "False"`), the script will only add new records or update existing ones
- **Full Reset**: Set `RESET = "True"` to delete and recreate the BigQuery tables with fresh data

## How It Works

1. The script authenticates with Magento using your credentials and 2FA code
2. It fetches customer and order data within the specified date range
3. For customer data, it processes addresses, attributes, and customer groups
4. For order data, it extracts order items and payment details
5. If incremental mode is enabled (default):
   - New records are added to BigQuery
   - Existing records are updated only if they've changed
6. If reset mode is enabled:
   - The existing tables are dropped and recreated
   - All fetched data is inserted as new

## Notes

- **Important**: You must create the BigQuery dataset and tables (customers and orders) before running the script
- The BigQuery service account needs appropriate permissions to create, delete, and modify tables
- For large data sets, consider using smaller date ranges to avoid timeout issues
- Magento API rate limits may apply depending on your server configuration
- Magento API documentation: https://doc.magentochina.org/swagger/#/salesOrderItemRepositoryV1/salesOrderItemRepositoryV1GetListGet