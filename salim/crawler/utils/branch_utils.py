import re

def branch_id(text: str | None) -> str:
    """
    Return a safe numeric branch id for filesystem use.
    Prefers leading digits (e.g., '970 ...' -> '970'), otherwise first digit run.
    Falls back to 'default' if no digits found.
    """
    if not text:
        return "default"
    text = text.strip()

    # prefer leading digits
    m = re.match(r"^\s*(\d{2,})", text)
    if m:
        return m.group(1)

    return "default"
