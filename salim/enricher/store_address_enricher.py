from __future__ import annotations
import os
import time
import sys
from typing import Optional, Dict, Any, Tuple, Union
import requests
from requests.exceptions import RequestException



NOMINATIM_URL = os.getenv("NOMINATIM_URL")
CONTACT = os.getenv("NOMINATIM_CONTACT")

HEADERS = {
    "User-Agent": f"StoreAddressEnricher/1.0 (+{CONTACT})"
}


def nominatim_search(query: str, city_hint: Optional[str] = None) -> Optional[Dict[str, Any]]:
    
    params = {
        "format": "json",
        "addressdetails": 1,
        "limit": 5,
        "q": query if not city_hint else f"{query} {city_hint}",
        "countrycodes": "il",
    }
    try:
        resp = requests.get(NOMINATIM_URL, params=params, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if data:
            return data[0]  
        return None
    except RequestException as e:
        print(f"[warn] Nominatim request failed for query={query!r}: {e}", file=sys.stderr)
        return None


def normalize_city(addr: Dict[str, Any]) -> Optional[str]:
    if not addr:
        return None
    for key in ("city", "town", "village", "municipality", "suburb"):
        if key in addr and addr[key]:
            return addr[key]
    return addr.get("county")


def format_full_address(addr: Dict[str, Any]) -> str:
    parts = []
    house = addr.get("house_number")
    road = addr.get("road") or addr.get("pedestrian") or addr.get("footway") or addr.get("residential")
    if road and house:
        parts.append(f"{road} {house}")
    elif road:
        parts.append(f"{road}")
    elif addr.get("neighbourhood"):
        parts.append(addr["neighbourhood"])

    city = normalize_city(addr)
    if city:
        parts.append(city)

    country = addr.get("country")
    if country:
        parts.append(country)

    return ", ".join(parts) if parts else (addr.get("display_name") or "")


def fetch_address_from_web(provider: str, store_id: Union[str, int], city_hint: Optional[str] = None) -> Optional[Tuple[str, str]]:

    store_id_str = str(store_id).strip()
    provider_clean = provider.strip()

    queries = [
        f"{provider_clean} סניף {store_id_str}",
        f"{provider_clean} branch {store_id_str}",
        f"{provider_clean} {store_id_str}",
        f"{provider_clean} רשת סופרמרקטים סניף {store_id_str}",
        f"{provider_clean} supermarket branch {store_id_str} Israel",
    ]

    for q in queries:
        result = nominatim_search(q, city_hint=city_hint)
        if result and "address" in result:
            addr = result["address"]
            city = normalize_city(addr) or ""
            full_addr = format_full_address(addr)
            if full_addr:
                time.sleep(1.1)
                return city, full_addr
        time.sleep(1.1)

    return None




def main(provider=None, store_id=None) -> int:

    if provider and store_id:
        print(f"[info] Searching address for provider={provider!r}, store_id={store_id!r}...")
        result = fetch_address_from_web(provider, store_id)
        if not result:
            print("[error] Address not found.")
            return 1

        city, address = result
        print(f"[ok] Found: city={city!r}, address={address!r}")
        
        return result

    return 0

