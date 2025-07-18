
import pytest
from lady_gaga_crawler import fetch_news_articles


def test_fetch_news_articles():
    url = 'https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X'
    articles = fetch_news_articles(url)
    assert isinstance(articles, list)
    assert len(articles) > 0
    for article in articles:
        assert 'title' in article
        assert 'description' in article
        assert 'date' in article
        assert 'image' in article
