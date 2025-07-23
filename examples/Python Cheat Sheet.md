# 🐍 Python Cheat Sheet – Web Crawler Basics

## 📁 File Handling

```python
# Write text file
with open("file.txt", "w") as f:
    f.write("Hello")

# Write binary file (e.g. image)
with open("file.jpg", "wb") as f:
    f.write(binary_data)

# Read file
with open("file.txt", "r") as f:
    content = f.read()
```

---

## 🌐 HTTP Requests with `requests`

```python
import requests

# Get HTML content
response = requests.get("https://example.com")
html = response.text

# Download binary data (e.g. image)
response = requests.get("https://example.com/image.jpg", stream=True)
for chunk in response.iter_content(1024):
    with open("image.jpg", "wb") as f:
        f.write(chunk)
```

---

## 🧪 Validating URLs

```python
def is_valid_url(url: str):
    return url and (url.startswith("http") or url.startswith("https"))
```

---

## 🧷 Paths with `os.path`

```python
import os

# Join path parts safely
path = os.path.join("folder", "file.txt")

# Save file in same folder as script
script_dir = os.path.dirname(__file__)
full_path = os.path.join(script_dir, "file.txt")
```

---

## 📦 JSON

```python
import json

# Serialize Python dict to JSON string
json_str = json.dumps(data)

# Write JSON to file
with open("file.json", "w") as f:
    f.write(json_str)
```

---

## ⚠️ Exception Handling

```python
try:
    risky_operation()
except Exception as e:
    print("Error occurred:", e)
```

---

## 🔁 Loops & Comprehensions

```python
# Loop through items
for item in items:
    print(item)

# List comprehension with condition
valid = [x for x in items if is_valid(x)]
```

---

## 🔣 String Manipulation

```python
# Join list of strings
"\n".join(["line1", "line2"])

# Get last part of URL
image_name = url.split("/")[-1]
```

---

## 🧠 Function Definition with Type Hints

```python
def download_image(url: str, path: str) -> None:
    ...
```

---

## 🚀 Script Entry Point

```python
if __name__ == "__main__":
    main()
```

---

## 🪄 Pro Tips

| Concept            | Code Sample                          |
| ------------------ | ------------------------------------ |
| Safe attribute get | `obj.get("key")`                     |
| Conditional return | `return val if val else default`     |
| Check truthiness   | `if my_list:` (empty list is False)  |
| Avoid nested ifs   | Use `and`/`or` in return expressions |
