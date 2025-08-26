# 📋 Development Guidelines

This document outlines the coding standards, best practices, and conventions for contributing to this data pipelines course repository.

---

## 📝 Commit Message Conventions

We follow [Conventional Commits](https://www.conventionalcommits.org/) specification:

### Format
```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types
- `feat`: New feature or functionality
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, missing semicolons, etc.)
- `refactor`: Code refactoring without changing functionality
- `test`: Adding or updating tests
- `chore`: Build process, dependencies, or tooling changes

### Examples
```bash
feat(crawler): add Lady Gaga news scraper
fix(pipeline): handle empty CSV files gracefully
docs(readme): update installation instructions
refactor(scraper): extract common parsing logic
test(pipeline): add unit tests for data validation
```

### Scope Guidelines
- `crawler`: Web scraping functionality
- `pipeline`: Data processing pipelines
- `etl`: Extract, Transform, Load operations
- `config`: Configuration files
- `db`: Database operations
- `api`: API integrations

---

## 🐍 Python Best Practices

### Code Style
- Follow [PEP 8](https://pep8.org/) style guide
- Use meaningful variable and function names
- Maximum line length: 88 characters (Black formatter standard)
- Use type hints for function parameters and return values

```python
def scrape_articles(url: str, max_pages: int = 5) -> list[dict[str, str]]:
    """Scrape articles from a news website.
    
    Args:
        url: The base URL to scrape
        max_pages: Maximum number of pages to scrape
        
    Returns:
        List of article dictionaries with title, content, and date
    """
    pass
```

### Error Handling
- Use specific exception types
- Always include meaningful error messages
- Log errors appropriately

```python
try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    logger.error(f"Failed to fetch {url}: {e}")
    raise ScrapingError(f"Unable to retrieve content from {url}")
```

### Documentation
- Use docstrings for all classes and functions
- Include parameter types and descriptions
- Document complex business logic with inline comments

---

## 🕷️ Web Scraping Guidelines

### Ethical Scraping
- Respect rate limits (add delays between requests)
- Use appropriate User-Agent headers
- Don't overload servers with concurrent requests

### Implementation Standards
```python
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class EthicalScraper:
    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(total=3, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': 'Educational-Scraper/1.0 (Course Assignment)'
        })
        return session
    
    def fetch_page(self, url: str) -> str:
        time.sleep(self.delay)  # Rate limiting
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return response.text
```

### Data Extraction
- Use CSS selectors or XPath for robust element selection
- Handle missing elements gracefully
- Validate extracted data before storing

```python
from bs4 import BeautifulSoup

def extract_article_data(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, 'html.parser')
    
    # Use specific selectors
    title_elem = soup.select_one('h1.article-title')
    title = title_elem.get_text(strip=True) if title_elem else "No Title"
    
    content_elem = soup.select_one('.article-content')
    content = content_elem.get_text(strip=True) if content_elem else ""
    
    # Validate required fields
    if not content:
        raise ValueError("Article content is missing")
    
    return {
        'title': title,
        'content': content,
        'scraped_at': datetime.now().isoformat()
    }
```

---

## 🔧 SOLID Principles in Python

### 1. Single Responsibility Principle (SRP)
Each class should have only one reason to change.

```python
# ❌ Bad: Multiple responsibilities
class ArticleProcessor:
    def scrape_articles(self, url: str): pass
    def clean_text(self, text: str): pass
    def save_to_database(self, articles: list): pass
    def send_email_notification(self): pass

# ✅ Good: Separate responsibilities
class ArticleScraper:
    def scrape_articles(self, url: str) -> list[dict]: pass

class TextCleaner:
    def clean_text(self, text: str) -> str: pass

class ArticleRepository:
    def save_articles(self, articles: list[dict]) -> None: pass

class NotificationService:
    def send_completion_notification(self) -> None: pass
```

### 2. Open/Closed Principle (OCP)
Classes should be open for extension but closed for modification.

```python
from abc import ABC, abstractmethod

class DataExtractor(ABC):
    @abstractmethod
    def extract(self, source: str) -> list[dict]: pass

class NewsExtractor(DataExtractor):
    def extract(self, url: str) -> list[dict]:
        # Implementation for news websites
        pass

class SocialMediaExtractor(DataExtractor):
    def extract(self, api_endpoint: str) -> list[dict]:
        # Implementation for social media APIs
        pass

class ScrapingPipeline:
    def __init__(self, extractor: DataExtractor):
        self.extractor = extractor
    
    def run(self, source: str) -> list[dict]:
        return self.extractor.extract(source)
```

### 3. Liskov Substitution Principle (LSP)
Subtypes must be substitutable for their base types.

```python
class DataStorage(ABC):
    @abstractmethod
    def store(self, data: list[dict]) -> bool: pass

class CSVStorage(DataStorage):
    def store(self, data: list[dict]) -> bool:
        # CSV implementation
        return True  # Returns same type as parent

class JSONStorage(DataStorage):
    def store(self, data: list[dict]) -> bool:
        # JSON implementation  
        return True  # Returns same type as parent
```

### 4. Interface Segregation Principle (ISP)
Clients should not depend on interfaces they don't use.

```python
# ❌ Bad: Fat interface
class DataProcessor(ABC):
    @abstractmethod
    def scrape(self): pass
    @abstractmethod
    def clean(self): pass
    @abstractmethod
    def analyze_sentiment(self): pass
    @abstractmethod
    def generate_report(self): pass

# ✅ Good: Segregated interfaces
class Scraper(ABC):
    @abstractmethod
    def scrape(self, url: str) -> list[dict]: pass

class DataCleaner(ABC):
    @abstractmethod
    def clean(self, data: list[dict]) -> list[dict]: pass

class SentimentAnalyzer(ABC):
    @abstractmethod
    def analyze_sentiment(self, text: str) -> float: pass
```

### 5. Dependency Inversion Principle (DIP)
Depend on abstractions, not concretions.

```python
# ❌ Bad: High-level module depends on low-level module
class DataPipeline:
    def __init__(self):
        self.scraper = NewsScraper()  # Concrete dependency
        self.storage = CSVStorage()   # Concrete dependency

# ✅ Good: Depend on abstractions
class DataPipeline:
    def __init__(self, scraper: DataExtractor, storage: DataStorage):
        self.scraper = scraper  # Abstract dependency
        self.storage = storage  # Abstract dependency
    
    def process(self, source: str) -> None:
        data = self.scraper.extract(source)
        self.storage.store(data)

# Usage with dependency injection
scraper = NewsScraper()
storage = JSONStorage()
pipeline = DataPipeline(scraper, storage)
```

---

## 📁 Project Structure

Organize your code following these patterns:

```
assignment/
├── scraper/
│   ├── __init__.py
│   ├── base_scraper.py      # Abstract base classes
│   ├── news_scraper.py      # Specific implementations
│   └── utils.py             # Helper functions
├── pipeline/
│   ├── __init__.py
│   ├── processors.py        # Data processing logic
│   └── storage.py          # Data storage abstractions
├── config/
│   └── settings.py         # Configuration management
├── tests/
│   ├── test_scraper.py
│   └── test_pipeline.py
├── requirements.txt
└── README.md
```

---

## 🧪 Testing Guidelines

- Write unit tests for all business logic
- Use descriptive test method names
- Follow the Arrange-Act-Assert pattern

```python
import pytest
from scraper.news_scraper import NewsScraper

class TestNewsScraper:
    def test_extract_article_title_success(self):
        # Arrange
        scraper = NewsScraper()
        html = '<h1 class="title">Test Article</h1>'
        
        # Act
        title = scraper.extract_title(html)
        
        # Assert
        assert title == "Test Article"
    
    def test_extract_article_title_missing_element(self):
        # Arrange
        scraper = NewsScraper()
        html = '<div>No title here</div>'
        
        # Act & Assert
        with pytest.raises(ValueError, match="Title not found"):
            scraper.extract_title(html)
```

---

## 📦 Dependencies

Keep dependencies minimal and well-documented:

```python
# requirements.txt
requests>=2.28.0
beautifulsoup4>=4.11.0
pandas>=1.5.0
pytest>=7.0.0
black>=22.0.0
```

---

## 🔍 Code Review Checklist

Before submitting your PR, ensure:

- [ ] Code follows PEP 8 style guidelines
- [ ] All functions have type hints and docstrings
- [ ] Error handling is implemented appropriately
- [ ] SOLID principles are applied where relevant
- [ ] Unit tests cover main functionality
- [ ] No hardcoded values (use configuration)
- [ ] Commit messages follow conventional format
- [ ] Code is well-organized in appropriate modules

---

## 📚 Additional Resources

- [PEP 8 Style Guide](https://pep8.org/)
- [Python Type Hints](https://docs.python.org/3/library/typing.html)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [SOLID Principles](https://realpython.com/solid-principles-python/)
- [Web Scraping Ethics](https://blog.apify.com/is-web-scraping-legal/)

---

Happy coding! 🚀