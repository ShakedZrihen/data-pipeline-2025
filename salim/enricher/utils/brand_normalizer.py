import os, json, re
from typing import Dict, List, Optional, Tuple
from .brand_lexicon import BRAND_ALIASES

SPACE_RE  = re.compile(r"\s+")
DASH_CHARS = "\u002D\u2010\u2011\u2012\u2013\u2014\u2015\u2212"  
DASH_RE   = re.compile(f"[{re.escape(DASH_CHARS)}]")
QUOTES_RE = re.compile(r'["׳״„“”′″]')

TRAILING_TOKENS_RE = re.compile(
    r"""
    (?:\s*[,/|]?\s*
       (?:\d+(?:\.\d+)?\s*(?:גרם|ג|ק["״]?ג|קג|מ["״]?ל|מל|ליטר|ל)
         |\d+\s*יח(?:ידות|')?
         |מארז(?:\s+זוג|\s+\d+)?
         |אריזה(?:ות)?
         |זוג|שלישייה|רביעייה
         |\d+\s*[xX×]\s*\d+
         |\d+%
         |ש"ח|\u20AA)
    )+$""",
    re.VERBOSE
)

def normalize_dashes(s: str) -> str:
    """Replace various unicode dashes with a simple '-'."""
    return DASH_RE.sub("-", s)

def normalize_spaces(s: str) -> str:
    """Collapse repeated whitespace and trim."""
    return SPACE_RE.sub(" ", s).strip()

def _norm(s: str) -> str:
    """Canonicalize punctuation/spacing for consistent matching."""
    s = s.strip()
    s = normalize_dashes(s)
    s = QUOTES_RE.sub('"', s)
    s = normalize_spaces(s)
    return s

def _casefold(s: str) -> str:
    return s.casefold().strip()

def _build_alias_map(extra_file: Optional[str] = None) -> List[Tuple[str, str]]:
    alias_map: Dict[str, str] = {}
    data = dict(BRAND_ALIASES)

    extra_path = extra_file or os.getenv("BRANDS_EXTRA_JSON")
    if extra_path and os.path.exists(extra_path):
        try:
            with open(extra_path, "r", encoding="utf-8") as f:
                extra = json.load(f)
            for norm_brand, aliases in extra.items():
                data.setdefault(norm_brand, set()).update(aliases)
        except Exception:
            pass

    for norm, aliases in data.items():
        for a in aliases:
            alias_map[_casefold(_norm(a))] = norm

    # Longest aliases first (e.g., "קרפור קלאסיק" before "קרפור")
    return sorted(alias_map.items(), key=lambda kv: len(kv[0]), reverse=True)

ALIAS_BY_LENGTH: List[Tuple[str, str]] = _build_alias_map()

def _strip_trailing_tokens(s: str) -> str:
    s = _norm(s)
    s = TRAILING_TOKENS_RE.sub("", s)
    return _norm(s)

def _remove_brand_fragment(name: str, brand_alias_cf: str) -> Optional[str]:
    name_norm = _norm(name); name_cf = _casefold(name_norm)

    # suffix: "... <brand>"
    if name_cf.endswith(" " + brand_alias_cf):
        return _norm(name_norm[: len(name_norm) - len(brand_alias_cf)])

    # suffix with dash: "... - <brand>"
    dash_form = " - " + brand_alias_cf
    if name_cf.endswith(dash_form):
        return _norm(name_norm[: len(name_norm) - len(dash_form)])

    # prefix with dash: "<brand> - ..."
    lead = brand_alias_cf + " - "
    if name_cf.startswith(lead):
        return _norm(name_norm[len(lead):])

    # exact match == brand only
    if name_cf == brand_alias_cf:
        return ""

    return None

def split_brand_from_name(
    raw_name: Optional[str],
    manufacturer_hint: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (clean_name, normalized_brand or None).
    - Uses manufacturer_hint first (if it maps to a known brand alias).
    - Falls back to scanning known aliases in the product name.
    - Always strips trailing sizes/packaging.
    """
    if not raw_name:
        return None, (manufacturer_hint or None)

    s = _norm(raw_name)

    # Prefer explicit manufacturer hint if it maps to a known brand
    if manufacturer_hint:
        hint_cf = _casefold(_norm(manufacturer_hint))
        for alias_cf, brand_norm in ALIAS_BY_LENGTH:
            if alias_cf == hint_cf:
                cleaned = _remove_brand_fragment(s, alias_cf)
                if cleaned is not None:
                    return _strip_trailing_tokens(cleaned), brand_norm
                break
        # If hint isn’t in lexicon, still try removing it literally
        cleaned = _remove_brand_fragment(s, hint_cf)
        if cleaned is not None:
            return _strip_trailing_tokens(cleaned), manufacturer_hint.strip()

    # Scan all known aliases
    for alias_cf, brand_norm in ALIAS_BY_LENGTH:
        cleaned = _remove_brand_fragment(s, alias_cf)
        if cleaned is not None:
            return _strip_trailing_tokens(cleaned), brand_norm

    # No brand detected; just clean suffixes
    return _strip_trailing_tokens(s), None

__all__ = ["split_brand_from_name", "normalize_dashes", "normalize_spaces"]
