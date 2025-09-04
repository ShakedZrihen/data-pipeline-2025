import os
import shutil

def get_download_dir(base_dir, supermarket):
    return os.path.join(base_dir, 'providers', supermarket, 'temp')

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def move_file(src, supermarket, branch_id, base_dir):
    dest = os.path.join(base_dir, 'providers', supermarket, branch_id)
    os.makedirs(dest, exist_ok=True)
    os.rename(src, os.path.join(dest, os.path.basename(src)))

def delete_temp_dir(temp_dir):
    try:
        shutil.rmtree(temp_dir)
    except Exception as e:
        print(f"Could not delete temp folder: {e}")
