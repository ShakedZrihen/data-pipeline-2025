import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from uploader.upload_to_s3 import upload_file_to_s3

# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class SuperPharmCrawler:
    def __init__(self, config):
        self.config = config
        self.base_url = config["base_url"]
        self.download_folder = os.path.join("downloads", self.config["provider"])
        os.makedirs(self.download_folder, exist_ok=True)

        # list of branches
        self.branches = {
            'סופר-פארם נאות שקמה',
            'סופר-פארם רוטשילד',
            'סופר-פארם קניון הזהב',
            'סופר-פארם נרקיסים ראשון-לציון',
            'סופר-פארם אסף הרופא',
            'סופר-פארם נוה-הדרים',
            'סופר-פארם שער ראשון',
            'סופר-פארם המכללה ראשל"צ',
            'סופר-פארם קניון ראשונים'   
        }
    
    def crawl(self):    
        TARGET_PAGES = [1, 2, 5, 6, 8, 9, 10, 11, 16, 17, 18, 25, 27]
        for page_num in TARGET_PAGES:
            page_url = f"{self.base_url}/?page={page_num}"
            print(f"\nCrawling Super-Pharm page {page_num} -> {page_url}")
            try:
                response = requests.get(page_url)
                response.raise_for_status()
            except requests.RequestException as e:
                print(f"Failed to fetch page {page_num}: {str(e)}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            rows = soup.select("table tr")
            if not rows:
                print("No rows found on this page. Skipping.")
                continue

            for row in rows:
                cells = row.find_all("td")
                if not cells or len(cells) < 5:
                    continue

                # Branch name is in the 5th <td>
                branch_name = cells[4].get_text(strip=True)

                # Normalize: collapse spaces, unify quotes
                branch_name = re.sub(r"\s+", " ", branch_name)  
                branch_name = branch_name.replace("״", '"').replace("”", '"').replace("“", '"')

                # Skip if branch not in allowed list
                if branch_name not in self.branches:
                    continue

                # Safe name for filesystem
                safe_branch_name = re.sub(r"[^\w\-א-ת]", "_", branch_name)

                # Download link
                link = row.find("a", href=re.compile(r"\.gz$"))
                if not link:
                    continue


                href = link["href"]
                filename = os.path.basename(href)

                if not (filename.lower().startswith("pricefull") or filename.lower().startswith("promofull")):
                    continue

                # Branch-specific folder
                branch_folder = os.path.join(self.download_folder, safe_branch_name)
                os.makedirs(branch_folder, exist_ok=True)

                self.download_file(href, filename, branch_folder)

        
    
    def download_file(self, href, filename, destination_folder):
        full_url = urljoin(self.config["base_url"], href)  # the download URL
        dest_path = os.path.join(destination_folder, filename)  # where to save
        print(f"Downloading {filename}...")
        try:
            response = requests.get(full_url, stream=True)
            response.raise_for_status()  # Raise exception for bad status codes
            # Only keep the latest file of the same type (price or promo)
            file_type_prefix = "priceFull" if filename.lower().startswith("pricefull") else "promoFull"
            for existing_file in os.listdir(destination_folder):
                if (
                    existing_file.endswith(".gz")
                    and existing_file != filename
                    and existing_file.lower().startswith(file_type_prefix.lower())
                ):
                    os.remove(os.path.join(destination_folder, existing_file))

            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Saved to {dest_path}")

            # Upload to S3
            provider = self.config["provider"]  # e.g., "superpharm"
            branch = os.path.basename(destination_folder)
            s3_key = f"providers/{provider}/{branch}/{filename}"
            upload_file_to_s3(dest_path, s3_key)

        
        except requests.RequestException as e:
            print(f"Error downloading {filename}: {str(e)}")

if __name__ == "__main__":
    config = {
        "provider": "superpharm",
        "base_url": "https://prices.super-pharm.co.il",
        "page_url": "https://prices.super-pharm.co.il/?page=1"
    }

    crawler = SuperPharmCrawler(config)
    crawler.crawl()