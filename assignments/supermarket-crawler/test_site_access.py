#!/usr/bin/env python3
"""
Simple test to check if we can access the gov.il site
"""

import requests
from bs4 import BeautifulSoup

def test_site_access():
    url = "https://www.gov.il/he/pages/cpfta_prices_regulations"
    
    print(f"Testing access to: {url}")
    
    try:
        # Make request
        response = requests.get(url, timeout=30)
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Site is accessible!")
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Print page title
            title = soup.find('title')
            if title:
                print(f"Page title: {title.text.strip()}")
            
            # Look for download links
            links = soup.find_all('a', href=True)
            download_links = []
            
            for link in links:
                href = link['href'].lower()
                if any(ext in href for ext in ['.xlsx', '.xls', '.csv', 'download']):
                    download_links.append({
                        'text': link.get_text(strip=True)[:50],
                        'url': link['href'][:80]
                    })
            
            print(f"\nFound {len(download_links)} potential download links:")
            for i, dl in enumerate(download_links[:5]):  # Show first 5
                print(f"{i+1}. {dl['text']}...")
                print(f"   URL: {dl['url']}...")
            
        else:
            print(f"❌ Failed to access site. Status: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_site_access()
