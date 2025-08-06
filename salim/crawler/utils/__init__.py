import os
import requests

def download_file_from_link(link, output_dir, filename=None):
    if filename is None:
        filename = os.path.basename(link)
    output_path = os.path.join(output_dir, filename)
    response = requests.get(link, stream=True, verify=False)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded to {output_path}")
        return output_path
    else:
        print(f"Failed to download. Status code: {response.status_code}")
        return None