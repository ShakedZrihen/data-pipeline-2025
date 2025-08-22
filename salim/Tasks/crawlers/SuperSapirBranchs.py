
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re

URL = "https://supersapir.binaprojects.com/Main.aspx"
OUT = "SuperSapirBranchs.txt"

def clean_name(s: str) -> str:
    # normalize spaces and strip common footnote symbols
    s = re.sub(r"\s+", " ", s).strip()
    s = s.rstrip("*")  # drop trailing asterisks
    return s

def main():
    opts = Options()
    opts.add_argument("--headless=new")
    driver = webdriver.Chrome(options=opts)
    driver.get(URL)

    # wait until any data rows exist
    WebDriverWait(driver, 15).until(
        EC.presence_of_all_elements_located((By.XPATH, "//tr[starts-with(@id,'tr')]"))
    )

    rows = driver.find_elements(By.XPATH, "//tr[starts-with(@id,'tr')]")
    seen = set()
    ordered = []

    for r in rows:
        if not r.is_displayed():  # skip hidden/duplicate blocks
            continue
        tds = r.find_elements(By.TAG_NAME, "td")
        if len(tds) < 3:
            continue
        raw = tds[2].text.strip()
        if not raw:
            continue
        name = clean_name(raw)
        if name not in seen:
            seen.add(name)
            ordered.append(name)

    driver.quit()

    with open(OUT, "w", encoding="utf-8") as f:
        for name in ordered:
            f.write(name + "\n")

    print(f"Saved {len(ordered)} unique branch names → {OUT}")

if __name__ == "__main__":
    main()
