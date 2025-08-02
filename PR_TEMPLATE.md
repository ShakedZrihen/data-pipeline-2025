## 📝 What does this PR do?

- [x] Implements the Lady Gaga crawler using Selenium and BeautifulSoup
- [x] Extracts title, description, date, and image from Google News
- [x] Stores results correctly in JSON format
- [x] Handles base64 image filtering to only include actual URLs
- [x] Converts timestamps to readable date format

## 🧪 How to test?

1. **Setup the environment:**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   pip install selenium beautifulsoup4 webdriver-manager
   ```

2. **Run the crawler:**
   ```bash
   cd "assignments/warm-up/assignment 1"
   python lady_gaga_news_crawler.py
   ```

3. **Expected behavior:**
   - Chrome browser will open (non-headless mode)
   - Manually solve any captcha/consent if prompted
   - Press Enter when page is loaded
   - Script will extract top 20 news articles
   - Results saved to `lady_gaga_news.json`

4. **Check the output:**
   - Open `lady_gaga_news.json` to verify articles are extracted
   - Each article should have: title, description, date, image (URL or null)

## 🙋 Questions / Comments

- **Challenge:** Google News blocks automated browsers, so manual intervention (solving captcha) may be required
- **Alternative:** Consider switching to Bing News if Google continues blocking
- **Image URLs:** Filtered out base64 encoded images to only include actual URLs
- **Date format:** Converted Unix timestamps to readable YYYY-MM-DD HH:MM:SS format
- **Robustness:** Added error handling for missing elements and varying HTML structures 