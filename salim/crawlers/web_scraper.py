import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from config import LOGIN_URL, LOGIN_WAIT, PROCESSING_TIMEOUT, SUPERMARKETS


class WebScraper:
    def __init__(self, driver, file_manager):
        self.driver = driver
        self.file_manager = file_manager
    
    def login_to_supermarket(self, supermarket_name):
        """Login to a specific supermarket"""
        username = SUPERMARKETS[supermarket_name]["username"]
        print(f"\nLogging in to {supermarket_name} (username: {username})")
        
        try:
            self.driver.get(LOGIN_URL)
            WebDriverWait(self.driver, LOGIN_WAIT).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Find and fill username
            username_field = self.driver.find_element(By.CSS_SELECTOR, "input[name='username']")
            username_field.clear()
            username_field.send_keys(username)
            
            # Find and click login button
            login_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            login_button.click()
            
            time.sleep(3)
            
            # Check if login successful
            current_url = self.driver.current_url
            if current_url != LOGIN_URL:
                print(f"SUCCESS: Logged in to {supermarket_name}")
                return True
            else:
                print(f"ERROR: Login failed for {supermarket_name}")
                return False
                
        except Exception as e:
            print(f"ERROR: Login error for {supermarket_name}: {e}")
            return False
    
    def wait_for_processing(self, timeout=PROCESSING_TIMEOUT):
        """Wait for processing indicator to disappear"""
        try:
            print("Waiting for data to load...")
            
            # Wait for processing indicator to disappear
            try:
                wait = WebDriverWait(self.driver, timeout)
                wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "#fileList_processing")))
                print("Processing complete")
            except TimeoutException:
                print("Processing timeout - continuing anyway")
            
            time.sleep(1)  # Minimal buffer
            return True
            
        except Exception as e:
            print(f"Warning: Processing wait error: {e}")
            time.sleep(2)  # Fallback
            return True
    
    def search_and_download_files(self, search_term, supermarket_name, max_files=2):
        """Search for specific file type, sort by date, and download first 2 files immediately"""
        try:
            print(f"Searching and downloading: {search_term}")
            
            # Find and use search bar
            try:
                # Common search input selectors
                search_selectors = [
                    "input[type='search']",
                    "input[placeholder*='search']", 
                    "input[placeholder*='Search']",
                    "input.search",
                    "#search",
                    "[name='search']",
                    "input[aria-label*='search']"
                ]
                
                search_input = None
                for selector in search_selectors:
                    try:
                        search_input = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if search_input.is_displayed():
                            break
                    except NoSuchElementException:
                        continue
                
                if not search_input:
                    print("ERROR: Could not find search input")
                    return []
                
                # Clear and type search term
                search_input.clear()
                search_input.send_keys(search_term)
                print(f"Typed '{search_term}' in search bar")
                
                # Wait for search results
                time.sleep(2)
                self.wait_for_processing(10)
                
            except Exception as e:
                print(f"ERROR: Search failed: {e}")
                return []
            
            # Sort by date (click twice for newest first)
            try:
                date_header = self.driver.find_element(By.XPATH, "//th[contains(text(), 'Date')]")
                date_header.click()
                time.sleep(0.5)
                date_header.click()
                print("Date sorted (newest first)")
                
                # Wait for sort to complete
                self.wait_for_processing(10)
            except Exception as e:
                print(f"Warning: Sort failed: {e}")
            
            # Download first max_files files immediately
            return self.download_search_results_immediately(search_term, supermarket_name, max_files)
            
        except Exception as e:
            print(f"ERROR: Search and download files failed: {e}")
            return []
    
    def download_search_results_immediately(self, search_term, supermarket_name, max_files=2):
        """Download files from search results immediately while they're visible"""
        try:
            print(f"Downloading first {max_files} files from {search_term} search results...")
            
            # Get all rows in table
            rows = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            if not rows:
                rows = self.driver.find_elements(By.CSS_SELECTOR, "tr")
            
            downloaded_files = []
            
            # Download first max_files files immediately
            for i, row in enumerate(rows[:20]):  # Check first 20 rows
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if not cells:
                        continue
                    
                    file_name = cells[0].text.strip()
                    if not file_name:
                        try:
                            link = cells[0].find_element(By.TAG_NAME, "a")
                            file_name = link.text.strip()
                        except:
                            continue
                    
                    # Check if this is a .gz file that matches our search
                    if file_name.lower().endswith('.gz') and search_term.lower() in file_name.lower():
                        print(f"Found and downloading {search_term} file #{len(downloaded_files)+1}: {file_name}")
                        
                        # Download immediately
                        success = self.download_file_from_row(row, file_name, supermarket_name)
                        downloaded_files.append({
                            'name': file_name,
                            'success': success
                        })
                        
                        if len(downloaded_files) >= max_files:
                            break
                        
                except Exception as e:
                    print(f"Error processing row: {e}")
                    continue
            
            print(f"Downloaded {len(downloaded_files)} {search_term} files")
            return downloaded_files
            
        except Exception as e:
            print(f"ERROR: Download search results failed: {e}")
            return []
    
    def download_file_from_row(self, row, file_name, supermarket_name):
        """Download file directly from the given row - store temporarily"""
        try:
            print(f"Downloading: {file_name}")
            
            # Get cells from the row
            cells = row.find_elements(By.TAG_NAME, "td")
            
            # Look for download link
            download_link = None
            for cell in cells:
                try:
                    links = cell.find_elements(By.TAG_NAME, "a")
                    for link in links:
                        href = link.get_attribute('href')
                        if href and '.gz' in href:
                            download_link = link
                            break
                    if download_link:
                        break
                except:
                    continue
            
            if download_link:
                download_link.click()
                
                # Wait for download to complete
                if self.file_manager.wait_for_download(file_name, timeout=15):
                    print(f"[DOWNLOADED] {file_name}")
                    return True
                else:
                    print(f"[FAILED] {file_name} - Download timeout")
                    return False
            else:
                # Try clicking on first cell (file name)
                try:
                    cells[0].click()
                    
                    # Wait for download to complete
                    if self.file_manager.wait_for_download(file_name, timeout=15):
                        print(f"[DOWNLOADED] {file_name}")
                        return True
                    else:
                        print(f"[FAILED] {file_name} - Download timeout")
                        return False
                except Exception as e:
                    print(f"[FAILED] {file_name} - Click failed: {e}")
                    return False
                
        except Exception as e:
            print(f"ERROR: Download failed for {file_name}: {e}")
            return False