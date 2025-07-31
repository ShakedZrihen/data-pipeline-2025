from abc import ABC, abstractmethod

class CrawlerBase(ABC):
    """
    Abstract base class for web crawlers.
    Subclasses must implement the abstract methods.
    """
    
    @abstractmethod
    def crawl(self, url: str) -> dict:
        """
        Abstract method that must be implemented by subclasses.
        Crawls the given URL and returns the extracted data.
        
        Args:
            url (str): The URL to crawl
            
        Returns:
            dict: Extracted data from the URL
        """
        pass
    
    @abstractmethod
    def parse_content(self, content: str) -> dict:
        """
        Abstract method for parsing the crawled content.
        
        Args:
            content (str): The raw content to parse
            
        Returns:
            dict: Parsed data
        """
        pass
    
    def save_data(self, data: dict, filename: str) -> None:
        """
        Concrete method - this can be used as is or overridden.
        
        Args:
            data (dict): Data to save
            filename (str): Name of the file to save to
        """
        # Implementation here
        pass

