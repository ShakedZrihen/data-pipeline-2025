import gzip
import os

# Folder where AWS CLI downloaded files
local_folder = r"C:\price-files"

# Walk through all files in the folder
for root, dirs, files in os.walk(local_folder):
    for file in files:
        if file.endswith(".gz"):
            file_path = os.path.join(root, file)
            print("="*80)
            print(f"📂 File: {file_path}")
            print("="*80)

            # Open and read .gz file
            with gzip.open(file_path, "rt", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    print(line.strip())
            print("\n")
