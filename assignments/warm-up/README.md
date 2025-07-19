# 🕵️‍♀️ Warm Up: Lady Gaga News Crawler

Your task is to implement a **web crawler** that scrapes Google News results for articles about **Lady Gaga**.

## 🌐 Target URL

https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X

> ⚠️ Note: Google may require dynamic rendering — consider using Selenium.

---

## 🎯 Goals

For each article on the page, extract:

- `title`
- `description`
- `date`
- `image` (URL)

Store the results as a list of dictionaries or JSON-like structure.

---

## 🧱 Folder Structure

Create your code inside:

```bash
assignments/warm-up
```

---

## 📁 Files You May Use

From `examples/simple-crawler/`, you may refer to or reuse:

- `bs4-example.py` – Example using BeautifulSoup
- `selenium-example.py` – Example using Selenium
- `utils/__init__.py` – Useful helpers (e.g., logging, saving data)

---

## 🧪 How to Test Your Crawler

- ✅ Run locally and print extracted results
- ✅ Submit code + results in your PR
- ✅ Use `unittest` or basic function tests if possible
- ❌ Don't hit Google too frequently – use `time.sleep()`

---

## ⚠️ Important Considerations

- Be gentle: Add delays (`time.sleep(1-2s)`) between interactions
- Handle missing fields (e.g., if image or date is missing)
- Make sure your code is readable and documented

---

## 🚀 Submission Instructions

1. Fork this repository
2. Add your solution under `examples/simple-crawler/lady-gaga/`
3. Commit with a clear message (e.g., `feat: add lady gaga crawler`)
4. Open a Pull Request to the original repository

---

## 🧠 Bonus

If you'd like, you can:

- Store results in a JSON/CSV file
- Use `headless` browser config with Selenium
- Add screenshots or output samples in your PR

Good luck 💃🎤
