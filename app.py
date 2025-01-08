import streamlit as st
import requests
import pandas as pd
import os
import time
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_KEY = os.getenv("DUNE_API_KEY")
QUERY_ID = os.getenv("DUNE_QUERY_ID")

if not API_KEY or not QUERY_ID:
    st.error("API_KEY or QUERY_ID not found in environment variables.")
    st.stop()

DUNE_API_URL = "https://api.dune.com/api/v1/query/{}/execute"
DUNE_STATUS_URL = "https://api.dune.com/api/v1/execution/{}/status"
DUNE_RESULTS_URL = "https://api.dune.com/api/v1/execution/{}/results"

# Helper functions
def execute_dune_query(api_key, query_id, symbol, period):
    url = DUNE_API_URL.format(query_id)
    headers = {"X-DUNE-API-KEY": api_key}
    payload = {"query_parameters": {"symbol": symbol, "period": period}}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def check_query_status(api_key, execution_id):
    url = DUNE_STATUS_URL.format(execution_id)
    headers = {"X-DUNE-API-KEY": api_key}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def get_query_results(api_key, execution_id):
    url = DUNE_RESULTS_URL.format(execution_id)
    headers = {"X-DUNE-API-KEY": api_key}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def save_data_as_parquet(data, output_dir, symbol, period):
    partition_dir = os.path.join(output_dir, f"symbol={symbol}", f"period={period}")
    os.makedirs(partition_dir, exist_ok=True)
    file_path = os.path.join(partition_dir, "data.parquet")
    pd.DataFrame(data).to_parquet(file_path, engine="fastparquet")
    return file_path

# Streamlit app
st.title("Dune Analytics Query Executor")

# User inputs
st.write("Examples of symbols you can use: `USDC`, `ETH`, `BTC`, `DAI`, `USDT`")
symbol = st.text_input("Enter Symbol", "")
period = st.selectbox("Select Period", ["day", "week", "month", "year"])
if st.button("Execute Query"):
    try:
        # Execute query
        st.info("Executing query...")
        result = execute_dune_query(API_KEY, QUERY_ID, symbol, period)
        execution_id = result['execution_id']
        
        # Poll query status
        while True:
            status = check_query_status(API_KEY, execution_id)
            state = status['state']
            st.write(f"Current state: {state}")

            if state == "QUERY_STATE_COMPLETED":
                st.success("Query completed successfully.")
                results = get_query_results(API_KEY, execution_id)
                rows = results.get("result", {}).get("rows", [])
                if rows:
                    output_dir = "./output"
                    file_path = save_data_as_parquet(rows, output_dir, symbol, period)
                    st.success(f"File saved: {file_path}")
                    
                    # Convert rows to a DataFrame for visualization
                    df = pd.DataFrame(rows)

                    # Display the data in a table
                    st.write("### Query Results")
                    st.dataframe(df)

                    # Bar chart for volume_usd
                    st.write("### Volume by Period")
                    st.bar_chart(df.set_index("period")["volume_usd"])

                    # Line chart for fees_usd
                    st.write("### Fees Collected by Period")
                    st.line_chart(df.set_index("period")["fees_usd"])

                    # Provide download link
                    with open(file_path, "rb") as file:
                        st.download_button(
                            label="Download Parquet File",
                            data=file,
                            file_name=f"{symbol}_{period}.parquet",
                            mime="application/octet-stream"
                        )
                else:
                    st.warning("No data returned from the query.")
                break
            elif state == "QUERY_STATE_FAILED":
                st.error("Query failed.")
                break
            else:
                time.sleep(10)
    except Exception as e:
        st.error(f"An error occurred: {e}")
