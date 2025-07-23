# 🥣 BeautifulSoup Cheat Sheet (bs4)

## 📦 Installation

```bash
pip install beautifulsoup4 lxml
```

---

## ✨ Basic Usage

```python
from bs4 import BeautifulSoup

# From HTML string
soup = BeautifulSoup(html, "lxml")  # or "html.parser"

# From local file
with open("file.html", encoding="utf-8") as f:
    soup = BeautifulSoup(f, "html.parser")
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

# Single CSS Selector
soup.select_one("article > p")

# Regex Filtering
import re
soup.find_all("p", text=re.compile("Gaga"))
soup.find_all("img", src=re.compile(r"\.jpg$"))
```

---

## 🔄 Navigation & Access

```python
tag = soup.find("div")

tag.name         # 'div'
tag.attrs        # {'class': 'example'}

tag.text         # inner text
tag.get_text()   # same as above
tag.get_text(strip=True)  # remove whitespace

tag["class"]     # direct attribute access
tag.get("href")  # safe attribute access

tag.parent
tag.find_next_sibling()
tag.find_previous_sibling()

tag.children      # direct children (generator)
tag.descendants   # all nested children (recursive)
tag.contents      # direct children (list)
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

## ❌ Removing Tags

```python
# Remove all <script> and <style> tags
for tag in soup(["script", "style"]):
    tag.decompose()
```

---

## 💬 Comments

```python
from bs4 import Comment
comments = soup.find_all(string=lambda text: isinstance(text, Comment))
```

---

## 🧼 Clean & Prettify

```python
soup.prettify()
tag.prettify()
```

---

## 💡 Tips

| Goal                     | Example                                              |
|--------------------------|------------------------------------------------------|
| Get all links            | `soup.find_all("a", href=True)`                      |
| Get attribute safely     | `tag.get("href", "")`                                |
| Filter by multiple attrs | `soup.find_all("div", {"id": "x", "class": "y"})`    |
| Remove whitespace        | `tag.get_text(strip=True)`                           |
| Debug element structure  | `print(tag.prettify())`                              |

---

## 📚 Resources

- [Official BeautifulSoup Docs](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
