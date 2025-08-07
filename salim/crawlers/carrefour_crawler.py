from base_crawler import crawl

if __name__ == "__main__":
    crawl(
        start_url="https://www.gov.il/he/pages/cpfta_prices_regulations/",
        download_base_url="https://prices.carrefour.co.il/"
    )
