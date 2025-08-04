# from consts import *
from browser_utils import *
from selenium.webdriver.common.by import By
import json

class Crawler:
    driver=""
    config = ""

    def __init__(self):
        self.driver = get_chromedriver()
        with open("config.json", "r", encoding="utf-8") as file:
            self.config = json.load(file)
        # soup = get_html_parser(driver, providers_url)
        # finish the get all providers and input them into the config.json (username, password, url)

    def crawl(self):
        """
        Fetch and process data from the target source.
        """
        for provider in self.config["providers"]:
            print(f"Crawling provider: {provider['name']}")
            try:
                soup = get_html_parser(self.driver, provider["url"])
                if provider["username"]!="none":
                    username_input = self.driver.find_element(By.NAME, "username")
                    username_input.send_keys(provider["username"])
                    if provider["password"] != "none":
                        password_input = self.driver.find_element(By.NAME, "password")
                        password_input.send_keys(provider["password"])

                    button = self.driver.find_element("id", "login-button")
                    button.click()
                    time.sleep(2)
                    soup = BeautifulSoup(self.driver.page_source, "html.parser")

                data = self.extract_data(soup, provider)

                self.save_file(data, provider)
                # self.upload_file()
            except Exception as e:
                print(f"Error crawling {provider['name']}: {e}")
        pass

    def extract_data(self, soup, provider):
        """
        Extract data from the BeautifulSoup object.
        :param soup: BeautifulSoup object containing the page content
        :return: Extracted data
        """
        if not soup:
            return None
        table_body = soup.select_one(provider["table-body-selector"])
        price_tr = ""
        promo_tr = ""
        for row in table_body.select("tr"):
            column_name = row.select_one(provider["name-selector"])

            if column_name:
                col_text = column_name.text.strip()
                if col_text.startswith("Price"):
                    price_tr = self.return_latest_row(row, price_tr, provider)
                elif col_text.startswith("Promo"):
                    promo_tr = self.return_latest_row(row, promo_tr, provider)

        return {
            "price": price_tr,
            "promo": promo_tr
        }
    
    def return_latest_row(self, row, latest_row, provider):
        """
        Compare the current row with the latest row and return the latest one.
        :param row: The current row to compare
        :param latest_row: The latest row found so far
        :return: The latest row based on the comparison
        """
        return row


    def save_file(self, data, provider):
        """
        Save the given data to a local file.
        :param data: The data to be saved
        :param provider: The provider information (used for naming the file)
        """
        print(f"Saving data for {provider['name']}...")
        # filename = f"{provider['name']}_data.json"
        # with open(filename, "w", encoding="utf-8") as file:
        #     json.dump(data, file, ensure_ascii=False, indent=4)
        # pass

    def upload_file(self, filepath):
        """
        Upload the file to a remote destination (e.g., cloud storage).
        :param filepath: Path to the file to be uploaded
        """
        pass
