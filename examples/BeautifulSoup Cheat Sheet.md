# 🥣 BeautifulSoup Cheat Sheet (bs4)

## 📦 Installation

```bash
pip install beautifulsoup4 lxml
```

---

## ✨ Basic Usage

```python
from bs4 import BeautifulSoup

soup = BeautifulSoup(html, "lxml")  # or "html.parser"
```

---

## 🔍 Finding Elements

```python
# Find first tag
soup.find("div")
soup.find("a", class_="btn")
soup.find(id="main")

# Find all matching tags
soup.find_all("div")
soup.find_all("a", href=True)
soup.find_all("span", text="Click me")

# CSS Selectors
soup.select("div.content")
soup.select("#main > .item")
soup.select("ul > li:nth-of-type(2)")
```

---

## 🔄 Navigation

```python
tag = soup.find("div")
tag.name         # 'div'
tag.attrs        # {'class': 'example'}

tag.text         # inner text
tag.get_text()   # same as above

tag["class"]     # get attribute
tag.get("href")  # get safely

tag.parent
tag.find_next_sibling()
tag.find_previous_sibling()
```

---

## 🧰 Modify HTML

```python
tag["class"] = "new-class"
tag.string = "Updated text"
new_tag = soup.new_tag("p")
new_tag.string = "Hello!"
tag.append(new_tag)
```

---

## 🧼 Clean & Prettify

```python
soup.prettify()
```

---

## 💡 Tips

| Goal                     | Example                                              |
|--------------------------|------------------------------------------------------|
| Get all links            | `soup.find_all("a", href=True)`                      |
| Get attribute safely     | `tag.get("href", "")`                                |
| Filter by multiple attrs | `soup.find_all("div", {"id": "x", "class": "y"})`    |

---

## 📚 Resources

- [Official BeautifulSoup Docs](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)