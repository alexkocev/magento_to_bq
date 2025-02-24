# Export Magento Orders to BigQuery
This script exports order data from Magento to Google BigQuery. It fetches orders between a specified date range, formats the data, and uploads it to BigQuery.

## Setup
1. Magento API Access:

- Obtain your Magento Base URL, username, password, and 2FA OTP code for authentication.
- Set these variables in your environment or .env file:
    - M2_BASE_URL
    - M2_USERNAME
    - M2_PASSWORD
    - M2_OTP_CODE (6-digit from Google Authenticator)

2. Google Cloud:
- Set up Google Cloud credentials and BigQuery parameters:
    - BQ_PATH_KEY (Path to the service account key JSON)
    - BQ_PROJECT_ID (Google Cloud project ID)
    - BQ_DATASET_ID (BigQuery dataset ID)
    - BQ_TABLE_ID (BigQuery table ID)
- Install the required libraries using:
```python
pip install -r requirements.txt
```

## How it Works
1. Fetch Orders from Magento:

- The script retrieves order data for a specified date range (from_date and to_date).
- It supports pagination and continues fetching orders until all data is retrieved.

2. Format the Data:
- The order data is formatted into a structured pandas DataFrame with fields like order ID, customer name, item details, etc.

3. Upload to BigQuery:
- The formatted data is then uploaded to Google BigQuery using the to_gbq method.


## 2FA (Two-Factor Authentication) Support
The script supports Magento's Google Authenticator 2FA. Ensure you input the correct OTP code each time to generate a new access token.
Example Usage
```python
from_date = "2024-01-01"
to_date = "2024-01-10"
fetch_all_orders(from_date, to_date)
```

## Notes
- Ensure that your BigQuery dataset and table are set up correctly.
- Modify the script as needed to adjust the date range and handle different use cases.
- Magento API documentation: https://doc.magentochina.org/swagger/#/salesOrderItemRepositoryV1/salesOrderItemRepositoryV1GetListGet