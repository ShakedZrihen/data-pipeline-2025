
import unittest
from lady_gaga_crawler import LadyGagaCrawler

class TestLadyGagaCrawler(unittest.TestCase):

    def setUp(self):
        self.crawler = LadyGagaCrawler('https://example.com/lady-gaga-news')

    def test_fetch_page(self):
        # Mocking requests.get would be ideal here
        html_content = self.crawler.fetch_page()
        self.assertIsNotNone(html_content)

    def test_parse_page(self):
        sample_html = "<html><body><h2>Headline 1</h2><h2>Headline 2</h2></body></html>"
        headlines = self.crawler.parse_page(sample_html)
        self.assertEqual(headlines, ['Headline 1', 'Headline 2'])

if __name__ == '__main__':
    unittest.main()
