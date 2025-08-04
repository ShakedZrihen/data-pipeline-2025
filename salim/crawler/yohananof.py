import os
import typing
from datetime import datetime

from base import Crawler
from crawl_type import CrawlerType
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class YohananofCrawler(Crawler):
    def __init__(self, url: str, username: str, store: str = "yohananof"):
        super().__init__()
        self.url = url
        self.username = username
        self.store = store

    def crawl(self):
        print(f"Navigating to {self.url}")
        driver = self.driver
        driver.get(self.url)

        self._sign_in(driver)
        table = self._get_table_rows(driver)
        table_rows = table.find_elements(by=By.TAG_NAME, value="tr")

        def extract_branch_and_date(row):
            try:
                branch, date = self.get_branch(row)
                return branch, date
            except Exception as _:
                return None, datetime.min

        table_rows = sorted(
            table_rows,
            key=lambda row: extract_branch_and_date(row)[1],  # sort by date
            reverse=True,  # latest date first
        )

        # Track branches with available data
        branches_price = self.get_all_branches(table_rows)
        branches_prom = branches_price.copy()

        for row in table_rows:
            if len(branches_price) == 0 and len(branches_prom) == 0:
                break

            branch, _ = self.get_branch(row)
            _, date, t = self.get_row_data(row)

            # Skip if this file type has already been downloaded
            if t == CrawlerType.PRICING and branch not in branches_price:
                print(f"skipped branch {branch}")
                continue
            if t == CrawlerType.PROMOTION and branch not in branches_prom:
                print(f"skipped branch {branch}")
                continue

            name = self.get_name_from_row(row)
            if not (name.startswith("PriceFull") or name.startswith("PromoFull")):
                continue

            td_list = row.find_elements(by=By.TAG_NAME, value="td")
            expand_btn = td_list[-1].find_element(by=By.TAG_NAME, value="a")
            expand_btn.click()

            # Wait until <tr class="details"> is present
            WebDriverWait(driver, 5).until(
                lambda d: any(
                    "details" in r.get_attribute("class")
                    for r in d.find_element(By.ID, "fileList")
                    .find_element(By.TAG_NAME, "tbody")
                    .find_elements(By.TAG_NAME, "tr")
                )
            )

            # Find the <tr class="details"> row after this one
            tbody = driver.find_element(By.ID, "fileList").find_element(
                By.TAG_NAME, "tbody"
            )
            all_rows = tbody.find_elements(By.TAG_NAME, "tr")
            found = False
            paginated_row = None
            for r in all_rows:
                if found and "details" in r.get_attribute("class"):
                    paginated_row = r
                    break
                if r == row:
                    found = True

            if not paginated_row:
                print(f"[ERROR] Could not find expanded row for: {name}")
                continue

            # Get file metadata
            _, date, t = self.get_row_data(row)
            self.download_file(paginated_row, name, date, t)

            # Remove branch from corresponding dict
            if t == CrawlerType.PRICING:
                branches_price.pop(branch, None)
            else:
                branches_prom.pop(branch, None)

    def get_name_from_row(self, row: WebElement) -> str:
        td_list = row.find_elements(by=By.TAG_NAME, value="td")
        name = str(
            td_list[0].find_element(by=By.TAG_NAME, value="a").get_attribute("title")
        )
        return name

    def download_file(self, row: WebElement, name: str, date: str, t: CrawlerType):
        price_type = "price" if t == CrawlerType.PRICING else "promo"
        # start download
        btn_group = row.find_elements(by=By.CLASS_NAME, value="btn-group")
        if not btn_group:
            return
        btn_group[0].find_element(by=By.TAG_NAME, value="a").click()

        self.wait_for_any_download_complete(self.download_dir)
        to_save = self.format_filename_to_folder(name, price_type)
        self.move_file(self.download_dir, f"./salim/crawler/data/{to_save}")
        path = os.path.join("./salim/crawler/data", to_save)
        self.upload_s3(to_save, path)

    def get_row_data(self, row: WebElement) -> tuple[str, str, CrawlerType]:
        name = self.get_name_from_row(row)
        p_type = (
            CrawlerType.PRICING if name.startswith("Price") else CrawlerType.PROMOTION
        )
        date = name.split("-")[-1].split(".")[0]

        return (name, date, p_type)

    @typing.override
    def get_branch(self, row: WebElement) -> tuple[str, datetime]:
        td_list = row.find_elements(by=By.TAG_NAME, value="td")
        name = str(
            td_list[0].find_element(by=By.TAG_NAME, value="a").get_attribute("title")
        )
        branch = name.split("-")[1]
        date = name.split("-")[2].split(".")[0]
        date = self.parse_time(date)
        return branch, date

    def _get_table_rows(self, driver):
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//tbody[@class='context allow-dropdown-overflow']/tr")
            )
        )

        table = driver.find_element(by=By.ID, value="fileList").find_element(
            by=By.TAG_NAME, value="tbody"
        )
        return table

    def _sign_in(self, driver):
        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.TAG_NAME, "div"))
        )

        inputs = driver.find_elements(by=By.CLASS_NAME, value="input-group")
        username_elem = inputs[0].find_element(by=By.ID, value="username")
        username_elem.send_keys(self.username)
        driver.find_element(by=By.ID, value="login-button").click()


if __name__ == "__main__":
    crawler = YohananofCrawler("https://url.publishedprices.co.il/login", "yohananof")
    crawler.crawl()
