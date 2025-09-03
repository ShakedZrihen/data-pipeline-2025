import unittest
import base64
import binascii
from crawler import Crawler


class TestCrawler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("Running Init")
        crawler = Crawler(
            "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X"
        )
        cls.articles = crawler.crawl()

    def test_title(self):
        for art in self.articles:
            self.assertIsNotNone(art["title"])

    def test_desc(self):
        for art in self.articles:
            self.assertIsNotNone(art["desc"])

    @staticmethod
    def is_base64(s: str):
        try:
            # Try decoding (with padding correction)
            base64.b64decode(s, validate=True)
            return True
        except (binascii.Error, ValueError):
            return False

    def test_img(self):
        for art in self.articles:
            self.assertTrue(
                self.is_base64(art["image"].removeprefix("data:image/jpeg;base64,"))
            )


if __name__ == "__main__":
    unittest.main()
