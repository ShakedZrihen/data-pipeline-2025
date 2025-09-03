from goodpharm import GoodPharmCrawler


class CityMarketCrawler(GoodPharmCrawler):
    def __init__(self, url: str, store: str):
        super().__init__(url, store)


if __name__ == "__main__":
    CityMarketCrawler(
        "https://citymarketkiryatgat.binaprojects.com/Main.aspx", "citymarket"
    ).crawl()
