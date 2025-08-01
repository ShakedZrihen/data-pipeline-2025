# username = yohananof
# password = empty
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from app.crawlers.base import CrawlerBase

PROVIDER_URL = "https://url.publishedprices.co.il/file"
# Subclass זמני שמדלג על פעולות לא רלוונטיות
class DummyCrawler(CrawlerBase):
    def download_files_from_html(self, page_html = None):
        # נחזיר פשוט את התיקייה הידנית
        return r"C:\Users\97254\Documents\yearC\data-pipeline\data-pipeline-2025\salim\app\crawlers\local_files\yohananof"

# ניצור מופע
crawler = DummyCrawler(PROVIDER_URL)

# נריץ רק את החלק שרלוונטי לנו
crawler.run(PROVIDER_URL)
