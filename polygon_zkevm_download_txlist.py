import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time

# Update the API endpoint
ZKEVM_POLYGONSCAN_API_URL = "https://api-zkevm.polygonscan.com/api"

# Use API keys
API_KEYS_POLYGON_ZKEVM = [
    "API1",
    "API2",
    "API3",
    "API4",
    "API5",
    "API6"
]

def fetch_txlist_and_save(address, api_key):
    """
    Fetches the txlist for a given address using the provided API key.

    Returns the address and its txlist if it has a txlist record, otherwise returns None.
    """
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": api_key
    }

    while True:
        time.sleep(0.8)
        response = requests.get(ZKEVM_POLYGONSCAN_API_URL, params=params)

        if response.status_code == 200:
            data = response.json()
            if data["message"] == "OK" and data["result"]:
                return (address, data["result"])
            return None
        else:
            print(f"API error for address {address} with API key {api_key}. Retrying in 5 minutes.")
            time.sleep(300)  # Wait for 5 minutes

def fetch_all_addresses_and_save_txlist(addresses, api_keys, output_dir):
    """
    Fetches txlist records for all addresses using the provided API keys in parallel.

    Saves the txlist of each address in separate CSV files in the specified directory.

    """

    # Use a ThreadPoolExecutor to run the fetch_txlist_and_save function in parallel for all addresses
    with ThreadPoolExecutor(max_workers=len(api_keys)) as executor:
        futures = []

        # Distribute the addresses among the API keys
        for i, address in enumerate(addresses):
            api_key = api_keys[i % len(api_keys)]
            futures.append(executor.submit(fetch_txlist_and_save, address, api_key))

        # Collect the results and save txlist
        for future in futures:
            result = future.result()
            if result:
                address, txlist = result
                txlist_df = pd.DataFrame(txlist)
                txlist_df.to_csv(f"{output_dir}/{address}_txlist.csv", index=False)

    return

def get_valid_addresses_from_csv_files(directory):
    """
    Fetches valid addresses based on generated CSV files in the directory.
    """
    csv_files = [f for f in os.listdir(directory) if f.endswith('_txlist.csv')]
    return [extract_address_from_filename(f) for f in csv_files]

# Load addresses from CSV
df = pd.read_csv(r"D:\sybil\allocations.csv")

import os

def get_latest_csv_file(directory):
    # List all CSV files in the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith('_txlist.csv')]

    # Get the latest file based on creation time
    latest_file = max(csv_files, key=lambda f: os.path.getctime(os.path.join(directory, f)), default=None)

    return latest_file

def extract_address_from_filename(filename):
    return filename.replace('_txlist.csv', '')

# Specify the directory where you want to save txlists
output_directory = "D:\\sybil\\polygon_zkevm_txlists"

# Check if the directory exists, if not, create it
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Get the latest saved CSV file
latest_file = get_latest_csv_file(output_directory)

# If there's a latest file, find its position in the address list and continue from there
if latest_file:
    latest_address = extract_address_from_filename(latest_file)
    try:
        index = df["address"].tolist().index(latest_address)
        addresses_to_process = df["address"].tolist()[index + 1:]
    except ValueError:
        addresses_to_process = df["address"].tolist()
else:
    addresses_to_process = df["address"].tolist()

# Fetch txlist records for all addresses and save them
fetch_all_addresses_and_save_txlist(addresses_to_process, API_KEYS_POLYGON_ZKEVM, output_directory)

# Get valid addresses from generated CSV files
valid_addresses = get_valid_addresses_from_csv_files(output_directory)

# Save valid addresses to a CSV
valid_addresses_df = pd.DataFrame(valid_addresses, columns=["address"])
valid_addresses_df.to_csv(r"D:\sybil\polygon_zkevm_valid_addresses.csv", index=False)

# After processing all addresses
completed_file = os.path.join(output_directory, "completed.txt")
with open(completed_file, 'w') as f:
    f.write("Processing of all addresses completed.")