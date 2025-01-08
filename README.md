# Dune Query Executor

## Overview

The Dune Query Executor is a Python application that interacts with the Dune Analytics API to execute SQL queries, check their status, and retrieve results. The application is designed to facilitate data extraction and processing from Dune Analytics, allowing users to save the results in Parquet format.

## Features

- Execute SQL queries on Dune Analytics.
- Check the status of running queries.
- Retrieve query results and save them in Parquet and JSON formats.
- Environment variable management for API keys and query IDs.

## Requirements

- Python 3.7 or higher
- `requests` library
- `pandas` library
- `python-dotenv` library

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/Zuraiz270/dune-query-executor.git
   cd dune-query-executor
