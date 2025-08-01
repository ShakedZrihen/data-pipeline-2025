# username = yohananof
# password = empty

from salim.app.crawlers.base import CrawlerBase

# Subclass זמני שמדלג על פעולות לא רלוונטיות
class DummyCrawler(CrawlerBase):
    def download_files_from_html(self, page_html = None):
        # נחזיר פשוט את התיקייה הידנית
        return r"C:\Users\97254\Documents\yearC\data-pipeline\data-pipeline-2025\salim\app\crawlers\local_files\yohananof"

# ניצור מופע
crawler = DummyCrawler(provider_url=None)

# נריץ רק את החלק שרלוונטי לנו
crawler.run()
