# Lady Gaga Crawler

This script is designed to scrape Lady Gaga-related data from a specified website using BeautifulSoup and Requests.

## Setup

1. Ensure Python is installed.
2. Navigate to the 'examples/simple-crawler' directory.
3. Install dependencies using:
   ```
   pip install -r requirements.txt
   ```

## Usage

Run the crawler script with:
```
python lady_gaga_crawler.py
```

### Example Output

The crawler will extract headlines from the specified URL. For example, if the page contains:
```html
<h2>Latest News</h2>
<h2>Upcoming Tours</h2>
```
The output will be:
```
Extracted data: ['Latest News', 'Upcoming Tours']
```

## Testing

Run the tests with:
```
python -m unittest test_lady_gaga_crawler.py
```

## Configuration Options

- `url`: The URL of the website to scrape. Replace the placeholder URL in the script with the actual URL you wish to scrape.
