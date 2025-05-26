# fuel-prices-Airflow-project-LUXDEV
This code defines an Apache Airflow Directed Acyclic Graph (DAG) that performs an ETL (Extract, Transform, Load) process for gas price data in Nairobi, storing the results in a PostgreSQL database.
# Overview
The DAG named gas_price_etl_with_schema:

- Extracts gas price data from an external API
- Transforms the raw data into a clean format
- Loads it into a PostgreSQL database with proper schema management

# Components
# 1. PostgreSQL Configuration
Sets up connection parameters for the target database (host, credentials, etc.)

# 2. DAG Definition
- Scheduled to run daily (@daily)
- Starts from January 1, 2024
- Has retry configuration (1 retry with 5-minute delay)
- Tagged for organization (gas, postgres, etl)

# 3. ETL Steps
# Extract (extract_gas_price function)
- Makes an HTTPS request to collectapi.com's gas price API for Nairobi
- Saves raw JSON response to /tmp/gas_price_data.json
- Uses an API key for authentication
# Transform (transform function)
- Reads the raw JSON file
- Cleans the data:
        - Removes "TL" and commas from price values
        - Converts prices to numeric format
        - Adds a timestamp
- Creates a structured DataFrame
- Saves transformed data to CSV at /tmp/gas_price_transformed.csv
# Load (load_to_postgres function)
- Reads the transformed CSV
- Connects to PostgreSQL database
- Creates a schema (nairobi) and table (gas_prices) if they don't exist
        - Table has columns for fuel type, price, currency, and timestamp
- Inserts all transformed records
- Commits changes and closes the connection

# 4. Task Dependencies
- The tasks are chained in order:
extract_task >> transform_task >> load_task

# Output
The final output is clean gas price data stored in a PostgreSQL table with this structure:
- Schema: nairobi
- Table: gas_prices
- Columns: id, fuel_type, price, currency, timestamp
  ![2025-05-26 18 47 20](https://github.com/user-attachments/assets/92169c07-4089-4de9-adaa-d754ab5a4dc9)


