import json
from handler import handler

with open("event.json", "r", encoding="utf-8") as f:
    event = json.load(f)

print(handler(event, None))
