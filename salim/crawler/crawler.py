# from consts import *
from browser_utils import *
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
                    login_form = soup.find("form")
                    if login_form:
                        username_input = login_form.find("input", {"name": "username"})
                        password_input = login_form.find("input", {"name": "password"})
                        username_input["value"] = provider["username"]
                        if provider["password"] != "none":
                            password_input["value"] = provider["password"]

                        self.driver.find_element_by_name("submit").click()
                        time.sleep(2)
                
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
            column_name = row.select(provider["name-selector"])

            if column_name and column_name[0].text.strip() == "Price":
                price_tr = self.return_latest_row(row, price_tr)
            elif column_name and column_name[0].text.strip() == "Promo":
                promo_tr = self.return_latest_row(row, promo_tr)

        return {
            "price": price_tr,
            "promo": promo_tr
        }
    

    def return_latest_row(row, latest_row):
        """
        Compare the current row with the latest row and return the latest one.
        :param row: The current row to compare
        :param latest_row: The latest row found so far
        :return: The latest row based on the comparison
        """


    def save_file(self, data, provider):
        """
        Save the given data to a local file.
        :param data: The data to be saved
        :param provider: The provider information (used for naming the file)
        """
        filename = f"{provider['name']}_data.json"
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        pass

    def upload_file(self, filepath):
        """
        Upload the file to a remote destination (e.g., cloud storage).
        :param filepath: Path to the file to be uploaded
        """
        pass
