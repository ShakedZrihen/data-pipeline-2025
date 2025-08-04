from consts import *
from browser_utils import *
class Crawler:
    driver=""

    def __init__(self):
        driver = get_chromedriver()
        # soup = get_html_parser(driver, providers_url)
        # finish the get all providers and input them into the config.json (username, password, url)

    def crawl(self):
        """
        Fetch and process data from the target source.
        """
        pass

    def save_file(self, data, filename):
        """
        Save the given data to a local file.
        :param data: The data to be saved
        :param filename: Name of the file to save the data in
        """
        pass

    def upload_file(self, filepath):
        """
        Upload the file to a remote destination (e.g., cloud storage).
        :param filepath: Path to the file to be uploaded
        """
        pass
