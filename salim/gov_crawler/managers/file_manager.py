import os, certifi, requests, urllib3
from pathlib import Path
from urllib.parse import urlparse
from requests.exceptions import SSLError
from utils.enums import ENUMS

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
        referer: str | None = None,
    ) -> str | None:
        superMarket = superMarket or "default"
        branch = branch or "default"
        dest_dir = self.root / superMarket / branch
        dest_dir.mkdir(parents=True, exist_ok=True)

        if not filename:
            filename = os.path.basename(urlparse(link).path) or "downloaded.file"
        out_path = dest_dir / filename

        s = session or requests.Session()
        if referer:
            s.headers["Referer"] = referer

        def _get(url, verify):
            return s.get(url, stream=True, verify=verify, allow_redirects=True, timeout=60)

        try:
            verify = certifi.where() if verify_cert else False
            resp = _get(link, verify)
        except SSLError:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            resp = _get(link, False)

        if resp.status_code != 200:
            print(f"Download failed: HTTP {resp.status_code}")
            return None

        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "text/html" in ctype:
            snippet = (resp.text or "")[:160].replace("\n", " ")
            print(f"Got HTML instead of a file (likely login/permission). Snippet: {snippet!r}")
            return None

        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(8192):
                if chunk:
                    f.write(chunk)
        print(f"File downloaded to {out_path}")
        return str(out_path)
