# 🕷️ Simple Crawler Examples

This folder contains examples of two different web scraping approaches using Python:

- [`bs4-example.py`](./bs4-example.py) – scraping using **BeautifulSoup**
- [`selenium-example.py`](./selenium-example.py) – scraping using **Selenium WebDriver**

Both scrapers are configured to crawl from:

👉 **https://prices.mega.co.il/**

This is a public-facing Israeli supermarket site that provides downloadable product price files.

---

## 📁 Folder Structure

```
utils/
├── __init__.py                 # Utility functions: download, extract, convert XML→JSON
├── bs4-example.py              # BeautifulSoup scraper example
├── selenium-example.py         # Selenium-based scraper with dropdown interaction
├── requirements.txt            # Required packages
├── .flake8                     # PEP8 linter config
├── README.md                   # You're here!
├── BeautifulSoup Cheat Sheet.md
├── Selenium Cheat Sheet.md
```

---

## 📘 Example Scripts

### 🥣 `bs4-example.py`

- Uses `requests` + `BeautifulSoup`
- Parses all download buttons
- Downloads `.gz` files
- Extracts and optionally converts XML to JSON

### 🧪 `selenium-example.py`

- Uses `Selenium` to control a browser
- Selects a specific branch by value (e.g. `option="0084"`)
- Waits for the page to load updated results
- Downloads the latest price files
- Extracts and converts them as needed

---

## 🧰 Utilities (`__init__.py`)

Shared utility functions:
- `download_file_from_link()`
- `extract_and_delete_gz()`
- `convert_xml_to_json()`

These are used by both scrapers.

---

## 📦 Installation

```bash
pip install -r requirements.txt
```

For Selenium, make sure you have a compatible **ChromeDriver** and **Google Chrome** installed. You can use `webdriver-manager` to manage drivers automatically.

---

## 📑 Cheat Sheets

Need help writing your own scrapers?

- [BeautifulSoup Cheat Sheet](./BeautifulSoup%20Cheat%20Sheet.md)
- [Selenium Cheat Sheet](./Selenium%20Cheat%20Sheet.md)

---

## 📄 License

MIT – Free to use and modify.