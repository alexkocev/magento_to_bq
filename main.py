#%%

# -------------------------
# -------- IMPORTS --------
# -------------------------
import os
import json
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas_gbq
from tqdm import tqdm


import config
# Magento 2 Credentials
M2_BASE_URL = config.M2_BASE_URL            
M2_ACCESS_TOKEN = config.M2_ACCESS_TOKEN
M2_USERNAME = config.M2_USERNAME
M2_PASSWORD = config.M2_PASSWORD

# BigQuery Credentials and Table Information
BQ_PATH_KEY = config.BQ_PATH_KEY
BQ_PROJECT_ID = config.BQ_PROJECT_ID
BQ_DATASET_ID = config.BQ_DATASET_ID
BQ_ORDER_TABLE_ID = config.BQ_ORDER_TABLE_ID
BQ_CUSTOMER_TABLE_ID = config.BQ_CUSTOMER_TABLE_ID

# Date Range for Data Fetching
FROM_DATE = config.FROM_DATE
TO_DATE = config.TO_DATE

# Reset BQ Tables (True to reset data in BigQuery, False if incremental load)
RESET = config.RESET

# ----------------------------
# ---    GET NEW M2 TOKEN ----
# ----------------------------

# Function to fetch data from Magento (with OTP)
def get_magento_token():
    # Get OTP code from user
    M2_OTP_CODE = input("Enter the current 6-digit OTP code from your Google Authenticator app: ")
    
    # Prepare the payload for 2FA
    payload = {
        "username": M2_USERNAME,
        "password": M2_PASSWORD,
        "otp": M2_OTP_CODE
    }

    # Make the POST request to the 2FA authentication endpoint
    response = requests.post(f"{M2_BASE_URL}/rest/V1/tfa/provider/google/authenticate", headers={"Content-Type": "application/json"}, data=json.dumps(payload))

    # Check if the authentication is successful
    if response.status_code == 200:
        access_token = response.json()  # This will be the token you need for subsequent requests
        print("Access token received.")
        return access_token
    else:
        print("Error fetching token:", response.text)
        return None
    
    
M2_ACCESS_TOKEN = get_magento_token()

# -------------------------------------------
# ----- FETCH ORDER AND ITEM DETAILS --------
# -------------------------------------------

# Headers for authentication
HEADERS = {
    "Authorization": f"Bearer {M2_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

def fetch_orders(from_date, to_date, page=1):
    """
    Fetches orders created between two dates.
    Uses pagination to fetch results.
    """
    url = (
        f"{M2_BASE_URL}/rest/V1/orders?"
        f"searchCriteria[filter_groups][0][filters][0][field]=created_at&"
        f"searchCriteria[filter_groups][0][filters][0][value]={from_date} 00:00:00&"
        f"searchCriteria[filter_groups][0][filters][0][condition_type]=from&"
        f"searchCriteria[filter_groups][1][filters][0][field]=created_at&"
        f"searchCriteria[filter_groups][1][filters][0][value]={to_date} 23:59:59&"
        f"searchCriteria[filter_groups][1][filters][0][condition_type]=to&"
        f"searchCriteria[pageSize]=50&"
        f"searchCriteria[currentPage]={page}"
    )

    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching orders:", response.text)
        return None

def format_order_data(orders_data):
    """
    Formats the retrieved order data into a structured dataframe.
    Each row corresponds to a single item in the order.
    """
    formatted_data = []

    for order in orders_data.get('items', []):
        order_id = order.get('entity_id')
        created_at = order.get('created_at')
        grand_total = order.get('grand_total')
        currency = order.get('order_currency_code')
        status = order.get('status')

        # Customer details
        customer_name = f"{order.get('customer_firstname', '')} {order.get('customer_lastname', '')}".strip()
        customer_email = order.get('customer_email')
        billing_address = order.get('billing_address', {})
        city = billing_address.get('city', '')
        country = billing_address.get('country_id', '')

        # Order details
        payment = order.get('payment', {})
        payment_method = payment.get('method', 'N/A')

        # Extract individual items and create a row for each
        items = order.get('items', [])
        for item in items:
            formatted_data.append({
                "Order_ID": order_id,
                "Date": created_at,
                "Order_Total": f"{grand_total} {currency}",
                "Order_Status": status,
                "Customer_Name": customer_name,
                "Customer_Email": customer_email,
                "City": city,
                "Country": country,
                "Payment_Method": payment_method,
                "Item_Name": item.get('name'),
                "SKU": item.get('sku'),
                "Quantity": item.get('qty_ordered'),
                "Price_per_Unit": f"{item.get('price')} {currency}",
                "Total_Item_Price": f"{item.get('row_total')} {currency}",
            })

    return pd.DataFrame(formatted_data)

def fetch_all_orders(from_date, to_date):
    """
    Fetches all orders between a given date range in an iterative way.
    It checks the last date from the fetched orders and continues until all orders are retrieved.
    """
    all_orders_data = []
    current_date = from_date
    page = 1
    while True:
        print(f"Fetching orders for {current_date} (page {page})...")
        orders_data = fetch_orders(current_date, to_date, page)
        
        if not orders_data or not orders_data.get('items'):
            print("No more orders found.")
            break

        all_orders_data.append(orders_data)
        # Check the last order date from the current batch of orders to adjust the current_date
        last_order_date = orders_data['items'][-1]['created_at']
        current_date = last_order_date.split('T')[0]  # Date part of the last order date
        
        page += 1
        time.sleep(1)

    # Combine all fetched data into a single DataFrame
    all_formatted_data = []
    for orders_data in all_orders_data:
        all_formatted_data.append(format_order_data(orders_data))
    
    return pd.concat(all_formatted_data, ignore_index=True) if all_formatted_data else pd.DataFrame()

# -------------------------------------------
# -------       FETCH CUSTOMER DATA     -----
# -------------------------------------------

def fetch_customers(from_date, to_date, page=1):
    url = (
        f"{M2_BASE_URL}/rest/V1/customers/search?"
        f"searchCriteria[filter_groups][0][filters][0][field]=updated_at&"
        f"searchCriteria[filter_groups][0][filters][0][value]={from_date} 00:00:00&"
        f"searchCriteria[filter_groups][0][filters][0][condition_type]=from&"
        f"searchCriteria[filter_groups][1][filters][0][field]=updated_at&"
        f"searchCriteria[filter_groups][1][filters][0][value]={to_date} 23:59:59&"
        f"searchCriteria[filter_groups][1][filters][0][condition_type]=to&"
        f"searchCriteria[pageSize]=50&"
        f"searchCriteria[currentPage]={page}"
    )

    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching customers: {response.text}")
        return None

def fetch_all_customer_groups():
    """
    Fetch all customer groups at once and return as a dictionary mapping ID to name
    """
    groups_dict = {}
    try:
        print("Fetching all customer groups...")
        url = f"{M2_BASE_URL}/rest/V1/customerGroups/search?searchCriteria[pageSize]=100"
        response = requests.get(url, headers=HEADERS)
        
        if response.status_code == 200:
            groups_data = response.json()
            for group in groups_data.get('items', []):
                group_id = group.get('id')
                group_code = group.get('code')
                groups_dict[group_id] = group_code
            print(f"Successfully fetched {len(groups_dict)} customer groups")
            return groups_dict
        else:
            print(f"Error fetching customer groups: {response.text}")
            return {}
    except Exception as e:
        print(f"Exception fetching customer groups: {str(e)}")
        return {}

def format_customer_data(customers_data, customer_groups):
    """
    Formats the retrieved customer data into a structured dataframe.
    Each row corresponds to a single customer with detailed information.
    
    Args:
        customers_data (dict): Raw customer data from Magento API
        customer_groups (dict): Dictionary mapping group IDs to group names
        
    Returns:
        DataFrame: Formatted customer data
    """
    formatted_data = []
    customer_count = len(customers_data.get('items', []))
    print(f"Beginning to format {customer_count} customers...")

    for i, customer in enumerate(customers_data.get('items', [])):
        if i % 50 == 0:  # Log progress every 50 customers
            print(f"Formatting customer {i+1}/{customer_count}...")
            
        # Basic customer information
        customer_id = customer.get('id')
        email = customer.get('email')
        firstname = customer.get('firstname')
        lastname = customer.get('lastname')
        created_at = customer.get('created_at')
        updated_at = customer.get('updated_at')
        
        # Get customer group information
        group_id = customer.get('group_id')
        group_name = customer_groups.get(group_id, f"Group {group_id}")
        
        # Get additional customer attributes
        custom_attributes = {attr.get('attribute_code'): attr.get('value') for attr in customer.get('custom_attributes', [])}
        
        # Extract subscription status
        is_subscribed = customer.get('extension_attributes', {}).get('is_subscribed', False)
        
        # Get addresses if available
        addresses = customer.get('addresses', [])
        
        # Initialize address variables
        default_billing_address = None
        default_shipping_address = None
        all_addresses = []
        
        # Process addresses
        for address in addresses:
            address_data = {
                'id': address.get('id'),
                'city': address.get('city', ''),
                'country_id': address.get('country_id', ''),
                'firstname': address.get('firstname', ''),
                'lastname': address.get('lastname', ''),
                'postcode': address.get('postcode', ''),
                'telephone': address.get('telephone', ''),
                'street': ' '.join(address.get('street', [])),
                'region': address.get('region', {}).get('region', ''),
                'default_billing': address.get('default_billing', False),
                'default_shipping': address.get('default_shipping', False)
            }
            
            all_addresses.append(address_data)
            
            if address.get('default_billing', False):
                default_billing_address = address_data
            
            if address.get('default_shipping', False):
                default_shipping_address = address_data
        
        # Extract address details
        billing_city = default_billing_address.get('city', '') if default_billing_address else ''
        billing_country = default_billing_address.get('country_id', '') if default_billing_address else ''
        billing_postcode = default_billing_address.get('postcode', '') if default_billing_address else ''
        billing_telephone = default_billing_address.get('telephone', '') if default_billing_address else ''
        billing_street = default_billing_address.get('street', '') if default_billing_address else ''
        billing_region = default_billing_address.get('region', '') if default_billing_address else ''
        
        shipping_city = default_shipping_address.get('city', '') if default_shipping_address else ''
        shipping_country = default_shipping_address.get('country_id', '') if default_shipping_address else ''
        shipping_postcode = default_shipping_address.get('postcode', '') if default_shipping_address else ''
        shipping_telephone = default_shipping_address.get('telephone', '') if default_shipping_address else ''
        shipping_street = default_shipping_address.get('street', '') if default_shipping_address else ''
        shipping_region = default_shipping_address.get('region', '') if default_shipping_address else ''
        
        # Format customer record
        formatted_data.append({
            "Customer_ID": customer_id,
            "Email": email,
            "First_Name": firstname,
            "Last_Name": lastname,
            "Full_Name": f"{firstname} {lastname}",
            "Created_At": created_at,
            "Updated_At": updated_at,
            "Group_ID": group_id,
            "Group_Name": group_name,
            "Is_Subscribed": str(is_subscribed),
            
            # Address information
            "Billing_Street": billing_street,
            "Billing_City": billing_city,
            "Billing_Region": billing_region,
            "Billing_Postcode": billing_postcode,
            "Billing_Country": billing_country,
            "Billing_Telephone": billing_telephone,
            
            "Shipping_Street": shipping_street,
            "Shipping_City": shipping_city,
            "Shipping_Region": shipping_region,
            "Shipping_Postcode": shipping_postcode,
            "Shipping_Country": shipping_country,
            "Shipping_Telephone": shipping_telephone,
            
            # Custom attributes
            "Gender": custom_attributes.get('gender', ''),
            "Date_Of_Birth": custom_attributes.get('dob', ''),
            
            # Add other important custom attributes
            "VAT_Number": custom_attributes.get('vat_id', ''),
            "Company": custom_attributes.get('company', ''),
            "Account_Status": custom_attributes.get('customer_activation', '1'),  # '1' typically means active
            "Total_Address_Count": len(addresses),
            "Account_Age_Days": None,  # This will be calculated later if needed
        })

    print(f"Completed formatting {customer_count} customers")
    return pd.DataFrame(formatted_data)

def fetch_all_customers(from_date, to_date):
    all_customers_data = []
    page = 1
    total_pages = 1
    
    while page <= total_pages:
        print(f"Fetching customers for date range {from_date} to {to_date} (page {page})...")
        customers_data = fetch_customers(from_date, to_date, page)
        
        if not customers_data or not customers_data.get('items'):
            print("No customers found for the specified date range.")
            break

        all_customers_data.append(customers_data)
        
        # Update total pages based on search criteria
        total_count = customers_data.get('total_count', 0)
        page_size = 100  # This should match the pageSize in the API call
        total_pages = (total_count + page_size - 1) // page_size
        
        print(f"Retrieved page {page} of {total_pages} (Total customers: {total_count})")
        page += 1
        time.sleep(1)  # To avoid hitting API rate limits

    print("All customer data fetched, beginning processing...")
    
    # Fetch all customer groups once (major performance improvement)
    customer_groups = fetch_all_customer_groups()

    # Combine all fetched data into a single DataFrame
    all_formatted_data = []
    for i, customers_data in enumerate(all_customers_data):
        print(f"Processing batch {i+1}/{len(all_customers_data)}...")
        all_formatted_data.append(format_customer_data(customers_data, customer_groups))
    
    df_customers = pd.concat(all_formatted_data, ignore_index=True) if all_formatted_data else pd.DataFrame()

    print(f"Customer data formatted, processing {len(df_customers)} records...")

    # Calculate account age if created_at exists
    if not df_customers.empty and 'Created_At' in df_customers.columns:
        try:
            print("Calculating account age...")
            # Convert string dates to datetime objects
            df_customers['Created_At_DT'] = pd.to_datetime(df_customers['Created_At'])
            current_time = pd.Timestamp.now()
            
            # Calculate account age in days
            df_customers['Account_Age_Days'] = (current_time - df_customers['Created_At_DT']).dt.days
            
            # Drop the temporary datetime column
            df_customers = df_customers.drop(columns=['Created_At_DT'])
            print("Account age calculation completed")
        except Exception as e:
            print(f"Warning: Could not calculate account age: {str(e)}")
    
    return df_customers
# -------------------------------------------
# -------          ETL FUNCTIONS        -----
# -------------------------------------------

# Function to check if BigQuery table is empty
def check_table_exists(table_id):
    try:
        # Try fetching the table to check if it exists
        table = client.get_table(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}")
        print(f"Table {table_id} exists.")
        return table
    except Exception as e:
        # Check if the error message indicates the table doesn't exist
        if "Not found" in str(e) or "notFound" in str(e):
            print(f"Table {table_id} does not exist.")
            return None
        else:
            # Re-raise the exception if it's not a "Not found" error
            print(f"Error checking table {table_id}: {str(e)}")
            raise
    

def fetch_existing_data_from_bq(table_id):
    try:
        # Check if the table has a schema by getting table metadata
        table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}"
        table = client.get_table(table_ref)
        
        # If table has no schema, return an empty DataFrame
        if not table.schema:
            print(f"Table {table_id} exists but has no schema. Returning empty DataFrame.")
            return pd.DataFrame()
        
        # If table exists and has a schema, query the data
        query = f"""
            SELECT *
            FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}`
        """
        query_job = client.query(query)
        df_existing = query_job.to_dataframe()
        return df_existing
    
    except Exception as e:
        # If the error is due to no schema, return an empty DataFrame
        if "does not have a schema" in str(e):
            print(f"Table {table_id} exists but has no schema. Returning empty DataFrame.")
            return pd.DataFrame()
        # For other errors, raise the exception
        else:
            print(f"Error fetching data from table {table_id}: {str(e)}")
            raise




# Function to create BigQuery table schema from Magento data (with table deletion)
def create_table_from_data(table_id, df_new):
    # Fetch data from Magento to derive schema (use your existing function)
    if df_new.empty:
        print("No new data from Magento to fetch schema.")
        return None
    
    # Get schema from the first row of the dataframe (use data column names)
    schema = [bigquery.SchemaField(col, "STRING") for col in df_new.columns]
    
    # Check if the table exists, and delete it before recreating
    try:
        # Check if table exists
        client.get_table(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}")
        print(f"Table {table_id} already exists. Deleting the table.")
        client.delete_table(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}")  # Delete the table
    except bigquery.exceptions.NotFound:
        print(f"Table {table_id} does not exist. Proceeding to create a new one.")
    
    # Recreate the table with the new schema
    table = bigquery.Table(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}", schema=schema)
    client.create_table(table)  # Create the table with the inferred schema
    print(f"Table {table_id} has been created with schema from Magento data.")
    return df_new



def reset_bigquery_table(table_id):
    print(f"Resetting data and schema in BigQuery table {table_id}...")
    
    # Step 1: Delete the table (this removes all data and schema)
    client.delete_table(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}", not_found_ok=True)
    print(f"Table {table_id} has been deleted.")
    
    # Step 2: Recreate the table with an empty schema
    schema = []  # Empty schema
    table = bigquery.Table(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}", schema=schema)
    client.create_table(table)  # Recreate the table
    print(f"Table {table_id} has been recreated with a new schema.")
    

# Compare and update the data in BQ table
def compare_and_update_data(df_new, df_existing, id_column):
    # Check if the DataFrames are empty
    if df_new.empty:
        print(f"No new data provided. Skipping comparison.")
        return pd.DataFrame(), pd.DataFrame()
    
    # Check if id_column exists in both DataFrames
    if id_column not in df_new.columns:
        print(f"Error: '{id_column}' column not found in new data. Available columns: {df_new.columns.tolist()}")
        return pd.DataFrame(), pd.DataFrame()
    
    if df_existing.empty:
        print(f"No existing data found. All new data will be treated as new records.")
        # Create a DataFrame with the same structure as df_new but empty
        df_empty = pd.DataFrame(columns=df_new.columns)
        # Ensure consistent data types for the merge
        if id_column in df_empty.columns:
            df_empty[id_column] = df_empty[id_column].astype(str)
        df_new[id_column] = df_new[id_column].astype(str)
        # Perform outer merge to identify new records
        df_combined = pd.merge(df_empty, df_new, on=id_column, how="outer", suffixes=("_old", "_new"), indicator=True)
        return df_combined[df_combined["_merge"] == "right_only"], pd.DataFrame()
    
    if id_column not in df_existing.columns:
        print(f"Error: '{id_column}' column not found in existing data. Available columns: {df_existing.columns.tolist()}")
        print("Treating all new data as new records.")
        # Create a new DataFrame with just the id_column
        df_empty = pd.DataFrame({id_column: []})
        df_empty[id_column] = df_empty[id_column].astype(str)
        df_new[id_column] = df_new[id_column].astype(str)
        df_combined = pd.merge(df_empty, df_new, on=id_column, how="outer", suffixes=("_old", "_new"), indicator=True)
        return df_combined[df_combined["_merge"] == "right_only"], pd.DataFrame()
    
    # Print the data types for debugging
    print(f"Data type of {id_column} in new data: {df_new[id_column].dtype}")
    print(f"Data type of {id_column} in existing data: {df_existing[id_column].dtype}")
    
    # Convert both DataFrames' ID columns to the same type (string)
    df_new = df_new.copy()
    df_existing = df_existing.copy()
    
    df_new[id_column] = df_new[id_column].astype(str)
    df_existing[id_column] = df_existing[id_column].astype(str)
    
    # Print the data types after conversion
    print(f"After conversion - Data type of {id_column} in new data: {df_new[id_column].dtype}")
    print(f"After conversion - Data type of {id_column} in existing data: {df_existing[id_column].dtype}")
    
    # Now perform the merge with consistent data types
    df_combined = pd.merge(df_existing, df_new, on=id_column, how="outer", suffixes=("_old", "_new"), indicator=True)
    
    # Find new and updated records
    new_records = df_combined[df_combined["_merge"] == "right_only"]
    updated_records = df_combined[df_combined["_merge"] == "both"]
    
    # Check for actual differences in content between old and new versions
    # Get all columns except the ID column and "_merge" indicator
    value_columns = [col for col in updated_records.columns 
                    if not col.endswith('_old') and not col.endswith('_new') 
                    and col != '_merge' and col != id_column]
    
    # For each value column, check if old and new values are different
    has_changes = False
    for col in value_columns:
        old_col = f"{col}_old"
        new_col = f"{col}_new"
        
        if old_col in updated_records.columns and new_col in updated_records.columns:
            # Check if any row has different values between old and new
            has_diff = (updated_records[old_col] != updated_records[new_col]).any()
            if has_diff:
                has_changes = True
                break
    
    # If no actual changes found, return empty DataFrame for updated_records
    if not has_changes:
        updated_records = pd.DataFrame()
    else:
        # Keep only rows where there are actual differences
        updated_records = updated_records[updated_records.filter(like="_new").ne(updated_records.filter(like="_old")).any(axis=1)]
    
    return new_records, updated_records


# Upload new records to BigQuery
def upload_to_bq(df_new, table_id):
    # Clean up the DataFrame by removing suffix columns and merge indicator
    cols_to_drop = [col for col in df_new.columns if col.endswith('_old') or col.endswith('_new') or col == '_merge']
    df_new = df_new.drop(columns=cols_to_drop)
    
    # Generate the full table ID
    table_full_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}"
    
    # Upload to BigQuery
    pandas_gbq.to_gbq(df_new, destination_table=table_full_id, project_id=BQ_PROJECT_ID, if_exists='append')
    print(f'New records uploaded successfully to table {table_id}!')
   
   
def update_existing_data_in_bq(df_updated, table_id, id_column):
    print(f"Starting to update {len(df_updated)} records in {table_id}...")

    # Convert the DataFrame to a list of dictionaries
    rows_to_update = df_updated.to_dict(orient='records')

    # Add a counter to track progress
    processed = 0
    total = len(rows_to_update)
    
    # Use BigQuery's MERGE query to update the table with modified records
    for row in rows_to_update:
        # Track columns that have already been processed to avoid duplicates
        processed_columns = set()
        
        # Initialize the lists to hold query clauses
        set_clause = []
        insert_columns = [id_column]  # Always insert the ID column
        insert_values = [f"'{row[id_column]}'"]

        # Loop over all columns in the row and check for '_new' versions first
        for col in row.keys():
            if col.endswith('_new'):
                base_col = col.replace('_new', '')
                
                # Skip if we've already processed this column or if it's the ID column
                if base_col in processed_columns or base_col == id_column:
                    continue
                
                # Mark this column as processed
                processed_columns.add(base_col)
                
                # Add SET clause to update the column in BigQuery
                # Use appropriate formatting and escaping for the value
                value = row[col]
                if value is not None:
                    # Escape single quotes in string values
                    if isinstance(value, str):
                        value = value.replace("'", "''")
                    set_clause.append(f"T.{base_col} = '{value}'")
                
        # Increment and log progress every 10 records
        processed += 1
        if processed % 10 == 0 or processed == total:
            print(f"Processed {processed}/{total} records ({processed/total*100:.1f}%)...")
                    
            
            
        # Prepare the SET clause and ensure it's not empty
        if not set_clause:
            print(f"No changes detected for {id_column} {row[id_column]}. Skipping update.")
            continue  # Skip if no changes detected
        
        # Construct the query with dynamic SET clause
        query = f"""
        MERGE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}` AS T
        USING (SELECT '{row[id_column]}' AS id) AS S
        ON T.{id_column} = S.id
        WHEN MATCHED THEN
            UPDATE SET
                {', '.join(set_clause)}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(insert_columns)}) 
            VALUES ({', '.join(insert_values)});
        """

        # Execute the query to update BigQuery
        try:
            client.query(query).result()  # Execute the query and wait for the result
            print(f'Updated {id_column} {row[id_column]} in BigQuery table {table_id}.')
        except Exception as e:
            print(f"Error updating {id_column} {row[id_column]}: {str(e)}")

# -------------------------------------------
# -------         MAIN FUNCTION         -----
# -------------------------------------------


def process_data_type(data_type, from_date, to_date, table_id, id_column):
    print(f"Processing {data_type} data...")
    
    if RESET == "True":
        reset_bigquery_table(table_id)


    # Step 1: Fetch new data based on data type
    if data_type == 'orders':
        df_new = fetch_all_orders(from_date, to_date)
    elif data_type == 'customers':
        df_new = fetch_all_customers(from_date, to_date)
    else:
        print(f"Unsupported data type: {data_type}")
        return
    
    if df_new.empty:
        print(f"No {data_type} data found for the specified date range.")
        return
    
    # Step 2: Check if the table exists
    table = check_table_exists(table_id)
    
    # Step 3: Process based on table existence and schema
    if table is None:
        # Table doesn't exist - create it with data
        print(f"BigQuery table {table_id} does not exist. Creating table schema from data...")
        create_table_from_data(table_id, df_new)
        # Upload all data as new
        upload_to_bq(df_new, table_id)
        print(f"Created new table {table_id} and uploaded {len(df_new)} records.")
    else:
        # Check if the table has a schema
        try:
            if not table.schema:
                # Table exists but has no schema - recreate it with data
                print(f"BigQuery table {table_id} exists but has no schema. Recreating with data...")
                create_table_from_data(table_id, df_new)
                # Upload all data as new
                upload_to_bq(df_new, table_id)
                print(f"Recreated table {table_id} with schema and uploaded {len(df_new)} records.")
                return
        except Exception as e:
            # If we can't check schema, we'll continue and handle errors in fetch_existing_data_from_bq
            print(f"Warning: Could not verify schema for table {table_id}: {str(e)}")
        
        print(f"BigQuery table {table_id} exists. Fetching existing data...")
        # Fetch existing data
        df_existing = fetch_existing_data_from_bq(table_id)
        
        # If df_existing is empty but the table exists (no schema or empty table)
        if df_existing.empty:
            print(f"Table {table_id} exists but is empty or has no schema. Creating schema from new data...")
            create_table_from_data(table_id, df_new)
            # Upload all data as new
            upload_to_bq(df_new, table_id)
            print(f"Recreated table {table_id} with schema and uploaded {len(df_new)} records.")
        else:
            # Normal flow - compare and update data
            print("Beginning BigQuery operations...") 
            new_records, updated_records = compare_and_update_data(df_new, df_existing, id_column)
            print(f"Data comparison complete. Found {len(new_records)} new and {len(updated_records)} updated records.")

            # Insert new records into BigQuery
            if not new_records.empty:
                print(f"Found {len(new_records)} new {data_type} to upload.")
                upload_to_bq(new_records, table_id)
            else:
                print(f"No new {data_type} found.")
                
            # Update existing records in BigQuery
            if not updated_records.empty:
                print(f"Found {len(updated_records)} {data_type} to update.")
                update_existing_data_in_bq(updated_records, table_id, id_column)
            else:
                print(f"No {data_type} updates found.")
    
    print(f"Completed processing {data_type} data.")
# -------------------------------------------
# -------             RUN               -----
# -------------------------------------------

# Set the Google Cloud credentials environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_PATH_KEY

# Initialize a BigQuery client
client = bigquery.Client(project=BQ_PROJECT_ID)

# Process customer data
process_data_type('customers', FROM_DATE, TO_DATE, BQ_CUSTOMER_TABLE_ID, "Customer_ID")
# Process order data
process_data_type('orders', FROM_DATE, TO_DATE, BQ_ORDER_TABLE_ID, "Order_ID")





















# %%
