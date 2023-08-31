import pandas as pd

# Load data
data = pd.read_csv(r"D:\sybil\polygon_zkevm_txlist_address_pairs.csv")

# Splitting the 'address_pair' column into 'sender' and 'receiver' columns
data[['sender', 'receiver']] = data['address_pair'].str.split('<->', expand=True)

address_mapping = {}

# Building the address mapping
for _, row in data.iterrows():
    sender, receiver = row['sender'].strip(), row['receiver'].strip()

    if sender not in address_mapping:
        address_mapping[sender] = set()
    address_mapping[sender].add(receiver)

    if receiver not in address_mapping:
        address_mapping[receiver] = set()
    address_mapping[receiver].add(sender)

# Function to perform Depth First Search to identify groups of addresses
def dfs(address, visited, current_group):
    visited.add(address)
    current_group.add(address)
    for neighbor in address_mapping[address]:
        if neighbor not in visited:
            dfs(neighbor, visited, current_group)
    return current_group

visited = set()
groups = []

# Identifying groups using DFS
for address in address_mapping:
    if address not in visited:
        current_group = dfs(address, visited, set())
        if len(current_group) >= 10:
            groups.append(current_group)

# Update output format for the TXT file
output_file_path = r"D:\sybil\polygon_zkevm_txlist_address_groups.txt"

with open(output_file_path, 'w') as f:
    for idx, group in enumerate(groups, start=1):
        f.write(f"GROUP {idx}\n```\n")
        for address in group:
            f.write(address + '\n')
        f.write("```\n\n")

print("Success!")
