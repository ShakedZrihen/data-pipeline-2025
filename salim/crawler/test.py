from browser_utils import *

if __name__ == "__main__":
    driver = get_chromedriver()
    soup = get_html_parser(driver, "https://www.gov.il/he/pages/cpfta_prices_regulations")
    print(f"HTML: {soup}")
