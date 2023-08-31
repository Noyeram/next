import pandas as pd
from collections import defaultdict
import os

# Load the address groups from the CSV
groups_df = pd.read_csv("D:/sybil/cronos_txlist_address_groups.csv")
groups = groups_df['address_groups'].tolist()

group_token_transfers = defaultdict(list)

# A set to keep track of seen transaction hashes
seen_hashes = set()

def get_transactions_from_file(address):
    """Read transactions for an address from a CSV file."""
    filepath = f"D:/sybil/cronos_txlists/{address}_txlist.csv"

    # If the file exists, read it into a DataFrame
    if os.path.exists(filepath):
        return pd.read_csv(filepath).to_dict('records')
    else:
        print(f"File for address {address} not found.")
        return []

for index, group in enumerate(groups, start=1):
    addresses = set(group.split(" <-> "))
    for address in addresses:
        transactions = get_transactions_from_file(address)
        for tx in transactions:
            # If the transaction hash has been seen before, we skip it
            if tx["hash"] in seen_hashes:
                continue
            # Add current transaction hash to the seen_hashes set
            seen_hashes.add(tx["hash"])
            if tx["from"] in addresses and tx["to"] in addresses and tx["from"] != tx["to"] and tx["input"] == "0x" and tx["value"] != 0:
                value = int(tx["value"]) / (10 ** 18)
                transfer_data = {"GROUP": index, "hash": tx["hash"], "from": tx["from"], "to": tx["to"], "tokenSymbol": "CRO", "value": value}
                group_token_transfers[index].append(transfer_data)

# Convert the results to a DataFrame
output_data = []
for idx, transfers in group_token_transfers.items():
    output_data.extend(transfers)
df_output = pd.DataFrame(output_data)

# Save the DataFrame to a CSV file
output_file_path = "D:/sybil/cronos_group_transactions.csv"
df_output.to_csv(output_file_path, index=False)

print("Success!")
