# 🕸️ Selenium Cheat Sheet (Python)

## 📦 Installation

```bash
pip install selenium
# Optional for managing drivers
pip install webdriver-manager
```

---

## 🚗 Basic Setup (Chrome)

```python
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument("--headless")  # Run without GUI (optional)

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

driver.get("https://example.com")
```

---

## 🔍 Finding Elements

```python
from selenium.webdriver.common.by import By

driver.find_element(By.ID, "element-id")
driver.find_element(By.CLASS_NAME, "btn")
driver.find_element(By.NAME, "q")
driver.find_element(By.TAG_NAME, "a")
driver.find_element(By.LINK_TEXT, "Click here")
driver.find_element(By.XPATH, "//div[@id='main']")
driver.find_element(By.CSS_SELECTOR, "div.content > a")

# Multiple elements
driver.find_elements(By.CLASS_NAME, "item")
```

---

## 🧭 Navigation and Actions

```python
driver.get("https://example.com")
driver.back()
driver.forward()
driver.refresh()
```

---

## 🎯 Interacting with Elements

```python
element = driver.find_element(By.NAME, "q")
element.send_keys("Selenium")
element.clear()
element.click()
```

---

## ⏳ Waits

```python
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Implicit wait
driver.implicitly_wait(10)

# Explicit wait
wait = WebDriverWait(driver, 10)
wait.until(EC.presence_of_element_located((By.ID, "some-id")))
```

---

## 📥 Handling Select Dropdowns

```python
from selenium.webdriver.support.ui import Select

select = Select(driver.find_element(By.ID, "dropdown"))
select.select_by_value("0084")
select.select_by_visible_text("Branch 84")
```

---

## 🪟 Switching Context

```python
driver.switch_to.frame("frame-name")
driver.switch_to.default_content()

driver.switch_to.window(driver.window_handles[1])
```

---

## 📸 Screenshots

```python
driver.save_screenshot("screenshot.png")
```

---

## ❌ Cleanup

```python
driver.quit()
```

---

## 💡 Tips

| Task                      | Code Snippet                                      |
|---------------------------|---------------------------------------------------|
| Wait for clickable button | `EC.element_to_be_clickable((By.ID, "btn"))`     |
| Check element exists      | `EC.presence_of_element_located`                 |
| Headless mode             | `options.add_argument("--headless")`             |
| Maximize window           | `driver.maximize_window()`                       |

---

## 📚 Resources

- [Selenium Python Docs](https://selenium-python.readthedocs.io/)
- [WebDriver Manager GitHub](https://github.com/SergeyPirogov/webdriver_manager)