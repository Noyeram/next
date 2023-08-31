import os
import time
import subprocess

# Define scripts and their corresponding output directories
scripts_and_directories = {
    "arb_nova_download_txlist.py": "D:\\sybil\\arb_nova_txlists",
    "base_download_txlist.py": "D:\\sybil\\base_txlists",
    "boba_download_txlist.py": "D:\\sybil\\boba_txlists",
    "cronos_download_txlist.py": "D:\\sybil\\cronos_txlists",
    "eth_download_txlist.py": "D:\\sybil\\eth_txlists",
    "fantom_download_txlist.py": "D:\\sybil\\fantom_txlists",
    "linea_download_txlist.py": "D:\\sybil\\linea_txlists",
    "polygon_zkevm_download_txlist.py": "D:\\sybil\\polygon_zkevm_txlists"
}

def get_last_modified_time(directory):
    """Returns the last modified time of a directory."""
    return max(os.path.getmtime(os.path.join(directory, f)) for f in os.listdir(directory))

def is_completed(directory):
    completed_file = os.path.join(directory, "completed.txt")
    return os.path.exists(completed_file)

def monitor_and_restart():
    """Monitors the output directories and restarts the corresponding scripts if no updates for 30 minutes."""

    processes = {}

    # Start all scripts initially
    for script, directory in scripts_and_directories.items():
        processes[script] = subprocess.Popen(["python", script])

    while True:
        completed_scripts = []
        for script, directory in scripts_and_directories.items():
            if is_completed(directory):
                print(f"{script} has completed processing all addresses. Not restarting.")
                completed_scripts.append(script)

        for script in completed_scripts:
            del scripts_and_directories[script]

        time.sleep(60)  # Check every 1 minutes

        # Check each directory
        for script, directory in scripts_and_directories.items():
            # If no update in the last 3 minutes, restart the script
            if time.time() - get_last_modified_time(directory) > 180:
                print(f"No updates in {directory} for the last 3 minutes. Restarting {script}...")
                if script in processes:
                    processes[script].terminate()
                processes[script] = subprocess.Popen(["python", script])

if __name__ == "__main__":
    monitor_and_restart()
