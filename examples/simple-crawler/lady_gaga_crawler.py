
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from loguru import logger

# Configure Selenium WebDriver
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

# Initialize WebDriver with error handling
try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
except Exception as e:
    logger.error(f"Failed to initialize WebDriver: {e}")
    raise

# URL to crawl
url = 'https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X'


def fetch_news_articles(url):
    articles = []
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'dbsr')))
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        for result in soup.find_all('div', class_='dbsr'):
            title = result.find('div', class_='JheGif nDgy9d').text
            description = result.find('div', class_='Y3v8qd').text
            date = result.find('span', class_='WG9SHc').text
            image = result.find('img')['src'] if result.find('img') else None

            articles.append({
                'title': title,
                'description': description,
                'date': date,
                'image': image
            })

        # Implement pagination handling
        next_button = driver.find_elements(By.ID, 'pnnext')
        if next_button:
            next_url = next_button[0].get_attribute('href')
            articles.extend(fetch_news_articles(next_url))

    except Exception as e:
        logger.error(f"Error fetching articles: {e}")

    return articles


if __name__ == '__main__':
    logger.info("Starting Lady Gaga news crawler")
    articles = fetch_news_articles(url)
    for article in articles:
        logger.info(f"Title: {article['title']}")
        logger.info(f"Description: {article['description']}")
        logger.info(f"Date: {article['date']}")
        logger.info(f"Image: {article['image']}")
        logger.info("-" * 80)

    driver.quit()
