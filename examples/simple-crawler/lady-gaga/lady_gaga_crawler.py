import json
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def init_chrome_driver():
    """Initialize Chrome driver with options"""
    chrome_options = Options()
    
    # Run in headless mode (no GUI)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Set language to English
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})
    
    # User agent
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    print("Setting up Chrome driver...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    return driver

def crawl_lady_gaga_news():
    """Crawl Lady Gaga news from Google using Selenium"""
    
    # URL with English parameters
    url = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&hl=en&gl=us"
    
    driver = init_chrome_driver()
    
    try:
        print(f"Navigating to: {url}")
        driver.get(url)
        
        # Wait for news results to load
        print("Waiting for page to load...")
        wait = WebDriverWait(driver, 10)
        
        # Wait for at least one news item
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.g, div[data-hveid], div.xuvV6b")))
        
        time.sleep(2)  # Additional wait for dynamic content
        
        articles = []
        
        # Try multiple selectors for news items
        news_items = driver.find_elements(By.CSS_SELECTOR, "div.g")
        if not news_items:
            news_items = driver.find_elements(By.CSS_SELECTOR, "div[data-hveid]")
        if not news_items:
            news_items = driver.find_elements(By.CSS_SELECTOR, "div.xuvV6b")
            
        print(f"Found {len(news_items)} potential news items")
        
        for idx, item in enumerate(news_items[:15]):  # Limit to first 15
            try:
                article = {
                    "title": None,
                    "description": None,
                    "date": None,
                    "image": None
                }
                
                # Extract title
                try:
                    title_elem = item.find_element(By.CSS_SELECTOR, "h3")
                    article["title"] = title_elem.text.strip()
                except:
                    # Try alternative selectors
                    try:
                        title_elem = item.find_element(By.CSS_SELECTOR, "div[role='heading']")
                        article["title"] = title_elem.text.strip()
                    except:
                        pass
                
                # Skip if no title
                if not article["title"]:
                    continue
                
                # Extract description
                try:
                    # Try to get text that's not the title
                    all_text = item.text
                    lines = all_text.split('\n')
                    for line in lines:
                        if line and line != article["title"] and len(line) > 20:
                            article["description"] = line.strip()
                            break
                except:
                    pass
                
                # Extract date
                try:
                    spans = item.find_elements(By.TAG_NAME, "span")
                    for span in spans:
                        text = span.text.lower()
                        if any(word in text for word in ['ago', 'hour', 'day', 'week', 'yesterday', 'minute', 'month']):
                            article["date"] = span.text.strip()
                            break
                except:
                    pass
                
                # Extract image
                try:
                    img = item.find_element(By.TAG_NAME, "img")
                    src = img.get_attribute("src") or img.get_attribute("data-src")
                    if src:
                        article["image"] = src
                except:
                    pass
                
                articles.append(article)
                print(f"Extracted article {idx+1}: {article['title'][:50]}...")
                
                # Be nice to Google
                time.sleep(0.5)
                
            except Exception as e:
                continue
        
        return articles
        
    except Exception as e:
        print(f"Error during crawling: {e}")
        return []
        
    finally:
        print("Closing browser...")
        driver.quit()

def save_results(articles):
    """Save results to JSON file"""
    with open("lady_gaga_news.json", "w", encoding="utf-8") as f:
        json.dump(articles, f, indent=2, ensure_ascii=False)
    print(f"\nResults saved to lady_gaga_news.json")

def main():
    """Main function"""
    print("=" * 60)
    print("Lady Gaga News Crawler (Selenium)")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Crawl the news
    articles = crawl_lady_gaga_news()
    
    # Display results
    print(f"\n{'=' * 60}")
    print(f"RESULTS: Found {len(articles)} articles")
    print("=" * 60)
    
    if articles:
        for i, article in enumerate(articles, 1):
            print(f"\nArticle {i}:")
            print(f"  Title: {article['title']}")
            if article['description']:
                print(f"  Description: {article['description'][:100]}...")
            print(f"  Date: {article['date'] or 'N/A'}")
            print(f"  Image: {'Yes' if article['image'] else 'No'}")
        
        save_results(articles)
        print("\n✅ Successfully extracted articles!")
    else:
        print("\n❌ No articles found.")
        print("Google might be showing a consent page or CAPTCHA.")
    
    print(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()