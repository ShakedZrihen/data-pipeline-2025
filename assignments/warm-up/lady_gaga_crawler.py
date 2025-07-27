import os
import time
import json
import platform
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager


def init_chrome_options():
    """Initialize Chrome options for headless browsing"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    return chrome_options


def get_chromedriver_path():
    """Get the correct chromedriver path for the current system"""
    try:
        # For macOS ARM64, we need to specify the architecture
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            print("Detected macOS ARM64, using specific chromedriver...")
            from webdriver_manager.core.os_manager import ChromeType
            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        else:
            driver_path = ChromeDriverManager().install()

        print(f"Chrome driver path: {driver_path}")
        return driver_path
    except Exception as e:
        print(f"Error with webdriver-manager: {e}")
        print("Falling back to system chromedriver...")
        return "chromedriver"


def extract_article_data(article_element):
    """Extract article data from a Google News article element"""
    try:
        title = ""
        title_selectors = [
            ".n0jPhd.ynAwRc.MBeuO.nDgy9d",
        ]
        
        for selector in title_selectors:
            try:
                title_element = article_element.find_element(By.CSS_SELECTOR, selector)
                if title_element and title_element.text.strip():
                    title = title_element.text.strip()
                    break
            except NoSuchElementException:
                continue
        
        # If no title found, try to get it from any text content
        if not title:
            try:
                # Get all text and find the first substantial line
                all_text = article_element.text.strip()
                lines = [line.strip() for line in all_text.split('\n') if line.strip()]
                if lines:
                    title = lines[0][:100]  # Limit to first 100 chars
            except:
                pass
        
        # Extract description/snippet
        description = ""
        desc_selectors = [
            ".GI74Re.nDgy9d",  
            
        ]
        
        for selector in desc_selectors:
            try:
                desc_element = article_element.find_element(By.CSS_SELECTOR, selector)
                if desc_element and desc_element.text.strip():
                    description = desc_element.text.strip()
                    break
            except NoSuchElementException:
                continue
        
        # Extract date - try multiple approaches
        date = ""
        date_selectors = [
            ".OSrXXb.rbYSKb.LfVVr",  
           
        ]
        
        for selector in date_selectors:
            try:
                date_element = article_element.find_element(By.CSS_SELECTOR, selector)
                if date_element and date_element.text.strip():
                    date = date_element.text.strip()
                    # Also try to get datetime attribute
                    datetime_attr = date_element.get_attribute("datetime")
                    if datetime_attr:
                        date = datetime_attr
                    # Try to get date from span inside the div
                    try:
                        span_element = date_element.find_element(By.TAG_NAME, "span")
                        if span_element:
                            span_text = span_element.text.strip()
                            if span_text:
                                date = span_text
                    except NoSuchElementException:
                        pass
                    break
            except NoSuchElementException:
                continue
        
        # Extract image URL
        image_url = ""
        img_selectors = [
            ".uhHOwf.BYbUcd img",
        ]
        
        for selector in img_selectors:
            try:
                img_element = article_element.find_element(By.CSS_SELECTOR, selector)
                if img_element:
                    src = img_element.get_attribute("src")
                    if not src:
                        src = img_element.get_attribute("data-src")
                    if src:
                        print(src)
                        image_url = src
                        break
            except NoSuchElementException:
                continue
        
        # Extract article URL
        article_url = ""
        link_selectors = [
            "a[href]", 
            ".title a", 
            "h3 a",
            ".LC20lb"
        ]
        
        for selector in link_selectors:
            try:
                link_element = article_element.find_element(By.CSS_SELECTOR, selector)
                if link_element:
                    href = link_element.get_attribute("href")
                    if href and href.startswith("http"):
                        article_url = href
                        break
            except NoSuchElementException:
                continue
        
        # Only return if we have at least a title
        if title:
            return {
                "title": title,
                "description": description,
                "date": date,
                "image_url": image_url,
                "article_url": article_url
            }
        
        return None
        
    except Exception as e:
        print(f"Error extracting article data: {e}")
        return None


def crawl_lady_gaga_news():
    """Crawl Google News for Lady Gaga articles"""
    url = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X"
    
    chrome_options = init_chrome_options()
    
    # Automatically download and manage Chrome driver
    print("Setting up Chrome driver...")
    try:
        chromedriver_path = get_chromedriver_path()
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        print("Trying alternative approach...")
        driver = webdriver.Chrome(options=chrome_options)
    
    articles = []
    seen_titles = set()  # To avoid duplicates
    
    try:
        print(f"Navigating to Google News: {url}")
        driver.get(url)
        
        # Wait for page to load
        print("Waiting for page to load...")
        time.sleep(3)
        
        # Wait for news articles to appear
        wait = WebDriverWait(driver, 10)
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-hveid], .g, .news-item")))
        except TimeoutException:
            print("Timeout waiting for news articles to load")
        
        # Look for article containers - Google News uses various selectors
        article_selectors = [
            "div[data-hveid]",  # Google News specific
            ".g",  # Google search results
            ".news-item",
            ".article",
            "[role='article']",
            ".dbsr"
        ]
        
        article_elements = []
        for selector in article_selectors:
            article_elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if article_elements:
                print(f"Found {len(article_elements)} articles using selector: {selector}")
                break
        
        if not article_elements:
            print("No articles found with standard selectors. Trying alternative approach...")
            # Try to find any div that might contain articles
            all_divs = driver.find_elements(By.TAG_NAME, "div")
            article_elements = [div for div in all_divs if div.text.strip() and len(div.text) > 50]
            print(f"Found {len(article_elements)} potential article containers")
        
        # Extract data from each article
        for i, article_element in enumerate(article_elements, 1):
            print(f"\nProcessing article {i}/{len(article_elements)}...")
            
            # Add delay between processing articles
            time.sleep(1)
            
            article_data = extract_article_data(article_element)
            if article_data and article_data["title"]:
                # Check for duplicates
                title_key = article_data["title"].lower().strip()
                if title_key not in seen_titles:
                    seen_titles.add(title_key)
                    articles.append(article_data)
                    print(f"✅ Extracted: {article_data['title'][:50]}...")
                else:
                    print(f"⏭️  Skipped duplicate: {article_data['title'][:50]}...")
            else:
                print(f"❌ Failed to extract article {i}")
        
        print(f"\nTotal unique articles extracted: {len(articles)}")
        
        # Save results
        output_dir = "lady_gaga_news"
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"lady_gaga_news_{timestamp}.json")
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Results saved to: {output_file}")
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"CRAWLING SUMMARY")
        print(f"{'='*60}")
        print(f"Total unique articles found: {len(articles)}")
        print(f"Output file: {output_file}")
        
        # Print first few articles as preview
        if articles:
            print(f"\nFirst 3 articles preview:")
            for i, article in enumerate(articles[:3], 1):
                print(f"\n{i}. {article['title']}")
                print(f"   Date: {article['date'] or 'Not available'}")
                print(f"   Description: {article['description'][:100] if article['description'] else 'Not available'}...")
        
    except Exception as e:
        print(f"Error during crawling: {e}")
    finally:
        driver.quit()
        print("Chrome driver closed.")
    
    return articles


if __name__ == "__main__":
    crawl_lady_gaga_news() 