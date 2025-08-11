import gzip
import json
import shutil
import os
import requests
import xml.etree.ElementTree as ET


def extract_and_delete_gz(gz_path):
    if not gz_path.endswith(".gz"):
        print("Not a .gz file:", gz_path)
        return None

    output_path = gz_path[:-3]

    with gzip.open(gz_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"Extracted to: {output_path}")

    # os.remove(gz_path)
    # print(f"Deleted: {gz_path}")
    # return output_path