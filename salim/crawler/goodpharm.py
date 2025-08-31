import os
import re

from base import Crawler
from crawl_type import CrawlerType
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class GoodPharmCrawler(Crawler):
    def __init__(self, url: str, store: str):
        super().__init__()
        self.url = url
        self.store = store

    def crawl(self):
        print(f"Navigating to {self.url}")
        driver = self.driver
        driver.get(self.url)

        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.TAG_NAME, "td"))
        )

        table_rows = driver.find_elements(by=By.TAG_NAME, value="tr")
        table_rows = table_rows[1:]
        price_type = CrawlerType.NONE

        branches_price = self.get_all_branches(table_rows)
        branches_prom = branches_price.copy()

        limit = int(os.getenv("LIMIT", 3))
        limit_cnt = 0

        for row in table_rows:
            if (
                len(branches_price) <= 0
                and len(branches_prom) <= 0
                or limit_cnt >= limit
            ):
                break

            (branch, _) = self.get_branch(row)
            if not branches_price.get(branch, None) and not branches_prom.get(
                branch, None
            ):
                continue

            name = self.get_name_from_row(row)
            if not (name.startswith("PriceFull") or name.startswith("PromoFull")):
                continue

            match price_type:
                case CrawlerType.NONE:
                    price_type, _ = self.download_file(row)
                    if price_type == CrawlerType.PRICING:
                        branches_price.pop(branch, None)
                    else:
                        branches_prom.pop(branch, None)
                case CrawlerType.PRICING:
                    limit_cnt += 1
                    price_type, _ = self.download_file(row)
                    branches_price.pop(branch, None)
                case CrawlerType.PROMOTION:
                    price_type, _ = self.download_file(row)
                    branches_prom.pop(branch, None)
                case _:
                    raise TypeError(
                        "Unknown Crawler type. expected CrawlerType.PRICING or CrawlerType.PROMOTION"
                    )

    def download_file(self, row: WebElement) -> tuple[CrawlerType, str]:
        filename, date, t = self.get_row_data(row)
        price_type = "price" if t == CrawlerType.PRICING else "promo"
        (
            row.find_elements(by=By.TAG_NAME, value="td")[5]
            .find_element(by=By.TAG_NAME, value="button")
            .click()
        )
        self.wait_for_any_download_complete(self.download_dir)
        to_save = self.format_filename_to_folder(filename, price_type)
        did_move = self.move_file(self.download_dir, f"./salim/crawler/data/{to_save}")
        path = os.path.join("./salim/crawler/data", to_save)

        if not did_move:
            return t, date

        self.upload_s3(to_save, path)
        return t, date

    @staticmethod
    def get_row_data(row: WebElement) -> tuple[str, str, CrawlerType]:
        td_list = row.find_elements(by=By.TAG_NAME, value="td")
        name = td_list[0].text
        p_type = td_list[2].text
        date = td_list[4].text

        prefix = re.match(r"^[^\d]+", name)
        if not prefix:
            prefix = ""
        else:
            prefix = prefix.group()

        pricing_type = (
            CrawlerType.PRICING if prefix == "PriceFull" else CrawlerType.PROMOTION
        )
        return (name, date, pricing_type)

    def get_name_from_row(self, row: WebElement) -> str:
        td_list = row.find_elements(by=By.TAG_NAME, value="td")
        name = td_list[0].text
        return name


if __name__ == "__main__":
    GoodPharmCrawler(
        "https://goodpharm.binaprojects.com/Main.aspx", "goodpharm"
    ).crawl()
