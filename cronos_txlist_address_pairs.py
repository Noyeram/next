import pandas as pd
import os

# 1. Load the first .csv file
valid_addresses_df = pd.read_csv("D:/sybil/cronos_valid_addresses.csv")
valid_addresses = set(valid_addresses_df['address'])

valid_pairs = set()

# 2. Open the file and process each .csv file inside
directory_path = "D:/sybil/cronos_txlists"
for file_name in os.listdir(directory_path):
    if file_name.endswith('.csv'):
        file_path = os.path.join(directory_path, file_name)

        # Read the csv file
        df = pd.read_csv(file_path)

        # 3. Check if the from and to addresses are in the first file
        mask = (df['from'].isin(valid_addresses) &
                df['to'].isin(valid_addresses) &
                (df['from'] != df['to']) &
                (df['input'] == '0x') &
                (df['value'] != 0))
        for index, row in df[mask].iterrows():
            valid_pairs.add(f"{row['from']}<->{row['to']}")

# 4. Save the valid address pairs to a single .csv file
output_path = "D:/sybil/cronos_txlist_address_pairs.csv"
output_df = pd.DataFrame(list(valid_pairs), columns=["address_pair"])
output_df.to_csv(output_path, index=False)

print("Processing completed. Filtered address pairs saved to cronos_txlist_address_pairs.csv.")
