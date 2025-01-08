import requests
import pandas as pd
import os
import time
import json
from dotenv import load_dotenv

load_dotenv()

# API Configuration
API_KEY = os.getenv("DUNE_API_KEY")
QUERY_ID = os.getenv("DUNE_QUERY_ID")

if not API_KEY or not QUERY_ID:
    ValueError("API_KEY or QUERY_ID not found in environment variables.")

DUNE_API_URL = "https://api.dune.com/api/v1/query/{}/execute"
DUNE_STATUS_URL = "https://api.dune.com/api/v1/execution/{}/status"
DUNE_RESULTS_URL = "https://api.dune.com/api/v1/execution/{}/results"

# Parameters
symbol = "USDT"  # (e.g., 'BTC', 'ETH', USDC', 'DAI', USDT')
period = "month"  # ('day', 'week', 'month', 'year')

def execute_dune_query(api_key, query_id, symbol, period):
    url = DUNE_API_URL.format(query_id)
    headers = {"X-DUNE-API-KEY": api_key}
    payload = {
        "query_parameters": {
            "symbol": symbol,
            "period": period
        }
    }
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as err:
        raise Exception(f"Error occurred: {err}")

def check_query_status(api_key, execution_id):
    url = DUNE_STATUS_URL.format(execution_id)
    headers = {"X-DUNE-API-KEY": api_key}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred while checking status: {http_err}")
    except requests.exceptions.RequestException as err:
        raise Exception(f"Error occurred while checking status: {err}")

def get_query_results(api_key, execution_id):
    url = DUNE_RESULTS_URL.format(execution_id)
    headers = {"X-DUNE-API-KEY": api_key}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred while retrieving results: {http_err}")
    except requests.exceptions.RequestException as err:
        raise Exception(f"Error occurred while retrieving results: {err}")

def save_data_as_parquet(data, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    df = pd.DataFrame(data)
    # Save data partitioned by symbol
    partition_dir = os.path.join(output_dir, f"symbol={symbol}")
    os.makedirs(partition_dir, exist_ok=True)
    file_path = os.path.join(partition_dir, f"data.parquet")
    df.to_parquet(file_path, engine="fastparquet")
    print(f"Data saved at {file_path}")

def save_json_as_file(data, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    json_file_path = os.path.join(output_dir, f"query_results.json")
    with open(json_file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    print(f"JSON data saved at {json_file_path}")

if __name__ == "__main__":
    try:
        # Execute Dune query
        result = execute_dune_query(API_KEY, QUERY_ID, symbol, period)
        
        execution_id = result['execution_id']
        print(f"Query is running. Execution ID: {execution_id}")
        
        # Check query status and wait for completion
        while True:
            status = check_query_status(API_KEY, execution_id)
            state = status['state']
            print(f"Current state: {state}")
            
            if state == "QUERY_STATE_COMPLETED":
                print("Query completed successfully.")
                results = get_query_results(API_KEY, execution_id)

                # Extract rows from results and save as Parquet file
                rows = results.get("result", {}).get("rows")
                if rows:
                    save_data_as_parquet(rows, "./output")
                else:
                    print("No data returned from the query.")
                break
            
            elif state == "QUERY_STATE_FAILED":
                print("Query failed.")
                break
            
            else:
                # Wait for 10 seconds before checking status again
                time.sleep(10)
                
    except Exception as e:
        print(f"Error: {e}")
