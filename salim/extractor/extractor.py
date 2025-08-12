import os
import zipfile

import boto3
import utils


class FileExtractor:
    def __init__(
        self,
        bucket: str = "test-bucket",
    ):
        self.bucket = bucket
        self.lmbda = boto3.client("lambda")
        self.s3_client = boto3.client("s3")

    def extract_single_file(self, s3_key: str, path: str = "/tmp"):
        # Save to /tmp because Lambda's /var/task is read-only
        local_path = os.path.join(path, os.path.basename(s3_key))
        paths = []
        try:
            self.s3_client.download_file(self.bucket, s3_key, local_path)
            print(f"downloaded file to {local_path}")
            if not self.check_gzip(local_path):
                print(f"extracting zip file: {local_path}...")
                paths = self.extract_delete_zip(local_path, "/tmp")
            else:
                print(f"extracting gz file: {local_path}...")
                p = utils.extract_and_delete_gz(local_path, "/tmp")
                paths = [p] if p else []
        except Exception as e:
            print(f"Error extracting file {local_path}: {e}, continuing...")
        print("finished file extraction.")
        return paths

    @staticmethod
    def extract_delete_zip(file_path: str, save_dir: str) -> list[str]:
        extracted_paths = []
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(save_dir)
            extracted_paths = [
                os.path.join(save_dir, name) for name in zip_ref.namelist()
            ]
        os.remove(file_path)
        return extracted_paths

    @staticmethod
    def check_gzip(file: str):
        with open(file, "rb") as f:
            magic_number = f.read(2)
            if magic_number == b"\x1f\x8b":
                return True
            else:
                return False
