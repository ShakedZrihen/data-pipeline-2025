import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from config import CHROME_OPTIONS, DOWNLOAD_PREFS


class BrowserManager:
    def __init__(self):
        self.driver = None
        self.download_dir = None
        self.setup_driver()
    
    def setup_driver(self):
        """Initialize Chrome driver with appropriate options"""
        # Create downloads directory
        self.download_dir = os.path.abspath("downloads")
        os.makedirs(self.download_dir, exist_ok=True)
        
        chrome_options = Options()
        
        # Add all chrome options from config
        for option in CHROME_OPTIONS:
            chrome_options.add_argument(option)
        
        # Set download preferences
        chrome_prefs = DOWNLOAD_PREFS.copy()
        chrome_prefs["download.default_directory"] = self.download_dir
        chrome_options.add_experimental_option("prefs", chrome_prefs)
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
        except Exception as e:
            try:
                driver_path = ChromeDriverManager().install()
                service = Service(driver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                print("Chrome driver initialized with webdriver-manager")
            except Exception as e2:
                print(f"Error initializing Chrome driver: {e2}")
                raise
    
    def get_driver(self):
        """Get the webdriver instance"""
        return self.driver
    
    def get_download_dir(self):
        """Get the download directory path"""
        return self.download_dir
    
    def close(self):
        """Close the webdriver"""
        if self.driver:
            self.driver.quit()
            print("Browser closed")