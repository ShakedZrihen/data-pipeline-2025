from __future__ import annotations
import os, re, json, time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

from dateutil import parser as dtparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ───────────────────────────── Globals ───────────────────────────── #
DATE_FMT          = "%d/%m/%Y"
_NOW              = datetime.now()              # single timestamp for run
_RE_RELATIVE      = re.compile(r"(\d+)\s+(minute|hour|day)s?\s+ago", re.I)
_RE_FALLBACK_SPAN = re.compile(r"(ago$|\b\d{4}\b|jan|feb|mar|apr|may|jun|"
                                r"jul|aug|sep|oct|nov|dec)", re.I)
_SELECTORS = (
    "span.r0bn4c.rQMQod",   # 2025 layout
    "div.OSrXXb span",      # old / alt layouts
    "span.rORZe",
    "span.CEMjEf span",
    "time"
)


# ──────────────────── Date normalisation helpers ─────────────────── #
def _normalize_date(raw: str | None) -> str | None:
    if not raw:
        return None
    txt = raw.strip().lower()

    # relative form (“18 hours ago”)
    if m := _RE_RELATIVE.match(txt):
        n, unit = int(m[1]), m[2].lower()
        delta = {"minute": timedelta(minutes=n),
                 "hour":   timedelta(hours=n),
                 "day":    timedelta(days=n)}[unit]
        return (_NOW - delta).strftime(DATE_FMT)

    # absolute form (“Jul 28, 2025” etc.)
    try:
        return dtparse.parse(txt).strftime(DATE_FMT)
    except Exception:
        return None


def _extract_date(card_soup: BeautifulSoup) -> Optional[str]:
    for sel in _SELECTORS:
        tag = card_soup.select_one(sel)
        if tag and tag.text.strip():
            if (d := _normalize_date(tag.text)):
                return d

    # heuristic fallback
    for span in card_soup.find_all("span"):
        if _RE_FALLBACK_SPAN.search(span.text):
            if (d := _normalize_date(span.text)):
                return d
    return None


# ───────────────────────── Selenium setup ────────────────────────── #
def _get_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    return webdriver.Chrome(options=opts)


# ─────────────────────────── Main scraper ────────────────────────── #
def scrape_google_news(query: str = "lady gaga in the news",
                       lang: str = "en",
                       country: str = "us",
                       timeout: int = 12) -> List[Dict]:
    url = (f"https://www.google.com/search?q={query.replace(' ', '+')}"
           f"&tbm=nws&hl={lang}&gl={country}")

    driver = _get_driver()
    try:
        driver.get(url)
        time.sleep(1)                       # <— politeness delay 💤

        # wait until at least one card wrapper appears
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "div.X7NTVe, div.pkphOe, div.SVJrMe, div.dbsr, "
                 "div.SoaBEf, div.Gx5Zad"))
        )

        card_divs = driver.find_elements(
            By.CSS_SELECTOR,
            "div.X7NTVe, div.pkphOe, div.SVJrMe, div.dbsr, div.SoaBEf, div.Gx5Zad"
        )

        results: List[Dict] = []
        for div in card_divs:
            soup = BeautifulSoup(div.get_attribute("innerHTML"), "html.parser")

            title_tag = (soup.select_one("h3") or
                         soup.select_one("div.JheGif.nDgy9d") or
                         soup.select_one("div[role='heading']"))
            link_tag  = soup.find("a", href=True)
            if not title_tag or not link_tag:
                continue

            desc_tag = (soup.select_one("div.Y3v8qd") or
                        soup.select_one("div.GI74Re") or
                        soup.select_one("div.MJNYkd"))
            img_tag = soup.find("img")

            results.append({
                "title":       title_tag.get_text(strip=True),
                "description": desc_tag.get_text(strip=True) if desc_tag else None,
                "date":        _extract_date(soup),
                "link":        link_tag["href"].replace("/url?q=", ""),
                "image":       img_tag.get("src") if img_tag else None
            })

        # Screenshot if nothing was found – handy for debugging CI
        if not results:
            driver.save_screenshot("debug_screenshot.png")

        return results

    finally:
        driver.quit()


# ────────────────────────── CLI entry‑point ───────────────────────── #
def main() -> None:
    data = scrape_google_news()

    # Locate repo‑root (first parent that already has "assignments")
    script_path = Path(__file__).resolve()
    repo_root = next((p for p in script_path.parents if (p / "assignments").is_dir()),
                     Path.cwd())

    out_dir  = repo_root / "assignments" / "warm-up"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "lady_gaga_news.json"

    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)

    print(json.dumps(data, indent=2, ensure_ascii=False))
    print(f"✅ Saved {len(data)} cards → {out_file}")


if __name__ == "__main__":
    main()
