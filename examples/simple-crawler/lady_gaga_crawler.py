
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Configure Selenium WebDriver
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

# Initialize WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

# URL to crawl
url = 'https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X'

def fetch_news_articles(url):
    driver.get(url)
    time.sleep(2)  # Allow time for the page to load
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    articles = []

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

    return articles

if __name__ == '__main__':
    articles = fetch_news_articles(url)
    for article in articles:
        print(f"Title: {article['title']}")
        print(f"Description: {article['description']}")
        print(f"Date: {article['date']}")
        print(f"Image: {article['image']}")
        print("-" * 80)

    driver.quit()
