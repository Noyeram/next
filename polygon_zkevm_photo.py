import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# Load the CSV file
data_all_groups = pd.read_csv("D:/sybil/polygon_zkevm_group_transactions.csv")

output_directory = "polygon_zkevm_photo"
os.makedirs(output_directory, exist_ok=True)


def shorten_address(address):
    return address[:6] + '...' + address[-6:]


def scale_positions(pos, scale_factor):
    """Scale the positions of nodes to increase the gap between them."""
    for key in pos:
        pos[key] = tuple([i * scale_factor for i in pos[key]])
    return pos


def get_shell_positions(G, addresses):
    num_addresses = len(addresses)
    if num_addresses < 30:
        return nx.shell_layout(G), (14, 10), 1
    elif num_addresses < 80:
        first_len = num_addresses * 2 // 10
        second_len = num_addresses * 3 // 10
        third_len = num_addresses - first_len - second_len
        return nx.shell_layout(G, [addresses[:first_len], addresses[first_len:first_len + second_len],
                                   addresses[first_len + second_len:]]), (14, 10), 1
    else:
        num_shells = (num_addresses // 30) + 1
        partitions = [10]
        while sum(partitions) < num_addresses:
            partitions.append(partitions[-1] + 8)
        partitions[-1] = num_addresses - sum(partitions[:-1])

        starts = [0] + [sum(partitions[:i]) for i in range(1, len(partitions))]
        shells = [addresses[starts[i]:starts[i] + partitions[i]] for i in range(len(partitions))]

        fig_size = (14 + 2 * (num_shells - 3), 10 + 1.5 * (num_shells - 3))
        scale_factor = 1 + 0.2 * (num_shells - 3)

        return nx.shell_layout(G, shells), fig_size, scale_factor


def plot_graph_for_group(index):
    relevant_rows = data_all_groups[data_all_groups['GROUP'] == index]
    addresses = relevant_rows['from'].tolist() + relevant_rows['to'].tolist()
    addresses = list(set(addresses))  # Remove duplicates
    G = nx.DiGraph()
    for address in addresses:
        G.add_node(address)
    for _, row in relevant_rows.iterrows():
        from_address = row["from"]
        to_address = row["to"]
        G.add_edge(from_address, to_address)
    labels = {node: shorten_address(node) for node in G.nodes()}

    pos, fig_size, scale_factor = get_shell_positions(G, addresses)
    pos = scale_positions(pos, scale_factor)

    # Create a figure and axis with more control
    fig, ax = plt.subplots(figsize=fig_size)
    nx.draw(G, pos, labels=labels, with_labels=True, node_size=3000, node_color="skyblue", font_size=12, width=2,
            edge_color="gray", arrowsize=20, arrowstyle='-|>', ax=ax)

    # Set the title on the axis, which should provide better control
    ax.set_title(f"Address Relationship for polygon_zkevm_txlist_group{index} with Shell Layout", fontsize=16, pad=20)

    output_path = os.path.join(output_directory, f"polygon_zkevm_txlist_group{index}.png")
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()


# Iterate over all unique groups and plot graphs
unique_groups = data_all_groups['GROUP'].unique()
for idx in unique_groups:
    plot_graph_for_group(idx)
