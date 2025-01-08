# Dune Query Executor

## Overview

The Dune Query Executor is a Python application that interacts with the Dune Analytics API to execute SQL queries, check their status, and retrieve results. The application is designed to facilitate data extraction and processing from Dune Analytics, allowing users to save the results in Parquet format.

Additionally, a **Streamlit-based web application** has been implemented and deployed to provide a user-friendly interface for executing queries and downloading results.

## Features

- Execute SQL queries on Dune Analytics.
- Check the status of running queries.
- Retrieve query results and save them in Parquet and JSON formats.
- Environment variable management for API keys and query IDs.
- **Streamlit Web App**:
  - User-friendly interface for entering query parameters (`symbol`, `period`).
  - Real-time updates on query execution status.
  - Downloadable Parquet file with query results.
  - Deployed for easy access without requiring a local setup.

## Requirements

- Python 3.7 or higher
- `requests` library
- `pandas` library
- `python-dotenv` library
- `streamlit` library
- `fastparquet` library (for saving Parquet files)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/Zuraiz270/dune-query-executor.git
   cd dune-query-executor
   ```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the project root and add your Dune API key and query ID:

   ```env
   DUNE_API_KEY=your_api_key
   DUNE_QUERY_ID=your_query_id
   ```

## Usage

### Command-Line Application

1. Update the `symbol` and `period` variables in `task.py` with your desired parameters.
2. Run the script:

   ```bash
   python task.py
   ```

   Results will be saved in the `output` directory in both JSON and Parquet formats.

### Streamlit Web App

1. Run the Streamlit app:

   ```bash
   streamlit run app.py
   ```

2. Open the app in your browser (usually at `http://localhost:8501`).
3. Enter the `symbol` and `period` parameters, execute the query, and download the results as a Parquet file.

### Deployed Web App

The Streamlit web app has been deployed and is accessible online. You can use it directly to execute Dune queries and download the results.

**Deployed App URL**:

## Features of the Web App

- **User-Friendly Interface**: Easily input parameters and execute queries.
- **Real-Time Feedback**: Get live updates on query execution status.
- **File Downloads**: Download query results directly in Parquet format.

## Example Workflow

1. Enter the desired `symbol` (e.g., BTC, ETH) and `period` (e.g., day, week, month, year) in the Streamlit app or directly in the script.
2. Execute the query.
3. Wait for the query status to update to "Completed."
4. Download the Parquet file containing the query results.

## Contributing

Feel free to fork the repository, open issues, or submit pull requests for new features or bug fixes.
