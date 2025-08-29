import os
from pathlib import Path
from urllib.parse import urlparse
from utils.enums import ENUMS
import certifi, requests

class FileManager:
    def __init__(self, local_root: str = ENUMS.LOCAL_DATA_DIR.value):
        self.base_dir = Path(__file__).resolve().parent.parent  
        self.root = self.base_dir / local_root
        self.root.mkdir(parents=True, exist_ok=True)  

    def download_to_branch(
        self,
        link: str,
        superMarket: str,
        branch: str,
        filename: str | None = None,
        session: requests.Session | None = None,
        verify_cert: bool = True,
    ) -> str | None:
        superMarket = superMarket or "default"
        branch = branch or "default"
        dest_dir = self.root / superMarket / branch
        dest_dir.mkdir(parents=True, exist_ok=True)  # per-branch folder
        if not filename:
            filename = os.path.basename(urlparse(link).path) or "downloaded.file"

        out_path = dest_dir / filename
        s = session or requests.Session()
        verify = certifi.where() if verify_cert else False

        resp = s.get(link, stream=True, verify=verify, allow_redirects=True)
        if resp.status_code != 200 or "text/html" in resp.headers.get("Content-Type", "").lower():
            print(f"Download failed: HTTP {resp.status_code}, or got HTML")
            return None

        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(8192):
                if chunk:
                    f.write(chunk)

        print(f"File downloaded to {out_path}")
        return str(out_path)
